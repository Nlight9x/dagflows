"""
DAG for daily securities data update
- Task 1: Download stock data from multiple sources (Vietstock, future connectors)
- Task 2: Push downloaded data to ClickHouse

IMPORTANT: Before running this DAG, create the ClickHouse table first.
See securities_table_schema.sql for the table schema.
"""
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Variable
from datetime import datetime, timedelta, time as dt_time
import pendulum
import json
import os
import time
import shutil
import pandas as pd

from utils.market_data_connector import VietstockConnector
from utils.storage_exporter import ClickHouseExporter


DEFAULT_DAG_CONFIG = {
    "shared_root": "/opt/airflow/shared",
    "shared_subdir": "stock",
    "keep_days": 5,
    "download_wait_seconds": 2,
    "export_batch_size": 1000,
}


def _get_data_date(**context):
    """Get trading date from Airflow context"""
    logical_date = context.get('logical_date') if 'logical_date' in context else datetime.now()
    return logical_date.date()


def _get_data_dir(dag_config, execution_date):
    shared_root = dag_config.get('shared_root')
    shared_subdir = dag_config.get('shared_subdir')
    date_str = execution_date.strftime("%Y-%m-%d")
    return os.path.join(shared_root, shared_subdir, date_str) if shared_root and shared_subdir else None


def _to_interval_label(interval_minutes):
    if interval_minutes <= 1:
        return "1m"
    elif interval_minutes % 1440 == 0:
        return f"{interval_minutes // 1440}d"
    elif interval_minutes % 60 == 0:
        return f"{interval_minutes // 60}h"
    else:
        return f"{interval_minutes}m"


def _load_dag_config(config_var_name):
    raw_config = Variable.get(config_var_name, default=None)
    if raw_config is None:
        raise ValueError(f"Airflow Variable '{config_var_name}' is not set. Please create it with DAG configuration.")
    try:
        cfg = json.loads(raw_config) if isinstance(raw_config, str) else raw_config
    except Exception as exc:
        raise ValueError(f"Failed to parse DAG config from Variable '{config_var_name}': {exc}")

    if not isinstance(cfg, dict):
        raise ValueError(f"DAG config must be a JSON object. Got: {type(cfg)}")

    merged = DEFAULT_DAG_CONFIG.copy()
    merged.update(cfg)

    symbols = merged.get('symbols')
    if not isinstance(symbols, list) or len(symbols) == 0:
        raise ValueError("DAG config must include non-empty 'symbols' list.")

    clickhouse_cfg = merged.get('clickhouse')
    if not isinstance(clickhouse_cfg, dict):
        raise ValueError("DAG config must include 'clickhouse' object with connection info.")
    for key in ["host", "port", "database", "user", "password"]:
        if key not in clickhouse_cfg:
            raise ValueError(f"ClickHouse config must include '{key}' field.")

    intervals = merged.get('intervals')
    if not isinstance(intervals, list) or len(intervals) == 0:
        raise ValueError("DAG config must include 'intervals' list.")
    for interval in intervals:
        if not isinstance(interval, dict):
            raise ValueError("Each interval definition must be an object with 'minutes' and 'table_name'.")
        if 'minutes' not in interval or 'table_name' not in interval:
            raise ValueError("Interval definition missing 'minutes' or 'table_name'.")

    return merged


def _cleanup_old_shared_data(dag_config, current_date, keep_days=5):
    """Remove data directories older than keep_days"""
    if keep_days <= 0:
        return
    shared_root = dag_config.get('shared_root')
    shared_subdir = dag_config.get('shared_subdir')
    base_dir = os.path.join(shared_root, shared_subdir)
    if not os.path.exists(base_dir):
        return

    cutoff_date = current_date - timedelta(days=keep_days - 1)
    for name in os.listdir(base_dir):
        dir_path = os.path.join(base_dir, name)
        if not os.path.isdir(dir_path):
            continue
        try:
            dir_date = datetime.strptime(name, '%Y-%m-%d').date()
        except ValueError:
            continue
        if dir_date < cutoff_date:
            shutil.rmtree(dir_path, ignore_errors=True)
            print(f"Removed old data directory: {dir_path}")


def download_securities_data(dag_config, **context):
    """
    Download securities data from multiple connectors (Vietstock, future: other connectors)
    Data is saved to shared folder with standardized format
    """
    execution_date = _get_data_date(**context)
    date_str = execution_date.strftime("%Y-%m-%d")

    keep_days = int(dag_config.get('keep_days', 5))
    wait_seconds = float(dag_config.get('download_wait_seconds', 2))

    data_dir = _get_data_dir(dag_config, execution_date)
    os.makedirs(data_dir, exist_ok=True)
    _cleanup_old_shared_data(dag_config, execution_date, keep_days=keep_days)

    symbols = dag_config.get('symbols')

    resolution = "1"

    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
    from_timestamp = str(int(date_obj.timestamp()))
    to_timestamp = str(int((date_obj + timedelta(days=1)).timestamp()))

    print(f"Downloading securities data for {len(symbols)} symbol(s): {', '.join(symbols)}")
    print(f"Date: {date_str}")
    print(f"Resolution: {resolution} (1 minute)")
    print(f"Output directory: {data_dir}")

    downloaded_files = []
    with VietstockConnector(timeout=30.0) as connector:
        for idx, symbol in enumerate(symbols):
            print(f"[{idx + 1}/{len(symbols)}] Downloading {symbol} from Vietstock...")

            try:
                df = connector.get_history(
                    symbol=symbol, resolution=resolution, from_timestamp=from_timestamp, to_timestamp=to_timestamp)

                output_filename = f"{symbol}_{date_str}.csv"
                output_path = os.path.join(data_dir, output_filename)

                df.to_csv(output_path, index=False)

                record_count = len(df)
                print(f"[{idx + 1}/{len(symbols)}] Saved {symbol}: {output_path} ({record_count} records)")

                downloaded_files.append({
                    'symbol': symbol, 'source': 'vietstock', 'file_path': output_path, 'record_count': record_count,
                    'status': 'success'})

                if idx < len(symbols) - 1 and wait_seconds > 0:
                    print(f"Waiting {wait_seconds} seconds before next request...")
                    time.sleep(wait_seconds)

            except Exception as e:
                print(f"Error downloading {symbol} from Vietstock: {e}")
                downloaded_files.append({
                    'symbol': symbol, 'source': 'vietstock', 'file_path': None, 'record_count': 0, 'status': 'failed',
                    'error': str(e)})

    metadata_path = os.path.join(data_dir, "metadata.json")
    metadata = {
        'date': date_str,
        'total_symbols': len(symbols),
        'downloaded_files': downloaded_files,
        'success_count': sum(1 for f in downloaded_files if f.get('status') == 'success'),
        'failed_count': sum(1 for f in downloaded_files if f.get('status') == 'failed')
    }

    with open(metadata_path, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)
        print(f"Metadata saved to: {metadata_path}")

    print(f"Successfully downloaded data for {metadata['success_count']}/{len(symbols)} symbol(s)")
    return metadata


def aggregate_minute_records(records: pd.DataFrame, interval_minutes):
    if records.empty or not interval_minutes or interval_minutes <= 1:
        return records.copy()

    df = records.copy()

    if 'datetime' not in df.columns:
        if 'timestamp' in df.columns:
            df['datetime'] = pd.to_datetime(df['timestamp'], unit='s')
        else:
            raise ValueError("Records must include 'datetime' or 'timestamp'.")

    df['datetime'] = pd.to_datetime(df['datetime'])
    df.sort_values('datetime', inplace=True)
    df.set_index('datetime', inplace=True)

    for col in ['open', 'high', 'low', 'close', 'volume']:
        if col not in df.columns:
            df[col] = None

    resampled = df.resample(f'{interval_minutes}T').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    })

    resampled.reset_index(inplace=True)
    resampled['timestamp'] = (resampled['datetime'].view('int64') // 10**9).astype(int)

    return resampled[['datetime', 'timestamp', 'open', 'high', 'low', 'close', 'volume']]


def export_records_to_clickhouse(exporter, records_df: pd.DataFrame, table_name, clickhouse_config, batch_size=1000):
    """Export a dataframe to ClickHouse table using ClickHouseExporter"""
    if records_df is None or records_df.empty:
        return {'exported_records': 0, 'status': 'skipped'}
    try:
        records = records_df.to_dict(orient='records')
        return exporter.export(
            data=records,
            batch_size=batch_size,
            column_mapping=None,
            keys=None,
            keys_mode='include'
        )
    finally:
        exporter.close()


def push_to_clickhouse(interval_minutes, table_name, dag_config, **context):
    """Push securities data to ClickHouse using DAG configuration"""
    execution_date = _get_data_date(**context)
    date_str = execution_date.strftime("%Y-%m-%d")
    interval_label = _to_interval_label(interval_minutes)

    clickhouse_config = dag_config.get('clickhouse')
    if isinstance(clickhouse_config, str):
        clickhouse_config = json.loads(clickhouse_config)
    if not isinstance(clickhouse_config, dict) or not clickhouse_config:
        raise ValueError("ClickHouse configuration missing in DAG config variable")

    data_dir = _get_data_dir(dag_config, execution_date)

    # if not data_dir or not os.path.exists(data_dir):
    #     raise FileNotFoundError(f"Data directory not found: {data_dir}. Please run download task first.")

    metadata_path = os.path.join(data_dir, "metadata.json") if data_dir else None
    if not metadata_path or not os.path.exists(metadata_path):
        raise FileNotFoundError(f"Metadata file not found: {metadata_path}. Please run download task first.")

    with open(metadata_path, 'r', encoding='utf-8') as f:
        metadata = json.load(f)

    print(f"Pushing {interval_label} data to ClickHouse table: {table_name}")
    print(f"Data directory: {data_dir}")
    print(f"Found {len(metadata['downloaded_files'])} files to process")

    total_exported = 0
    results = []

    clickhouse_config['table_name'] = table_name
    exporter = ClickHouseExporter(**clickhouse_config)
    exporter.delete_by_dates([execution_date])

    for file_info in metadata['downloaded_files']:
        if file_info.get('status') != 'success':
            continue
        file_path = file_info.get('file_path')
        if not file_path or not os.path.exists(file_path):
            continue
        symbol = file_info.get('symbol', 'UNKNOWN')

        try:
            if not file_path.endswith('.csv'):
                print(f"Skipping non-CSV file: {file_path}")
                continue

            df = pd.read_csv(file_path, parse_dates=['datetime'], usecols=lambda col: col != 'date')
            if df.empty or df['datetime'].isna().any():
                print(f"Skipping file due to invalid datetime values: {file_path}")
                continue

            if 'timestamp' not in df.columns:
                df['timestamp'] = (df['datetime'].view('int64') // 10**9).astype(int)

            records_df = aggregate_minute_records(df, interval_minutes)
            if records_df.empty:
                continue
            records_df['symbol'] = symbol
            records_df['date'] = records_df['datetime'].dt.date

            if records_df.empty:
                continue
            
            # print(records_df)
            batch_size = int(dag_config.get('export_batch_size', 1000))
            result = export_records_to_clickhouse(exporter, records_df, table_name, clickhouse_config, batch_size=batch_size)
            exported_count = result.get('exported_records', 0)
            total_exported += exported_count
            print(f"Exported {exported_count} {interval_label} records for {symbol} -> {table_name}")
            results.append({'symbol': symbol, 'exported_records': exported_count, 'status': 'success'})

        except Exception as e:
            print(f"Error processing {interval_label} data for {symbol}: {e}")
            results.append({'symbol': symbol, 'exported_records': 0, 'status': 'failed', 'error': str(e)})

    summary = {
        'date': date_str,
        'interval_minutes': interval_minutes,
        'table': table_name,
        'total_exported': total_exported,
        'processed_symbols': len(results),
        'success_count': sum(1 for r in results if r.get('status') == 'success'),
        'failed_count': sum(1 for r in results if r.get('status') == 'failed'),
        'results': results
    }
    print(f"{interval_label} Summary: Exported {total_exported} records to ClickHouse table '{table_name}'")
    return summary


# Set timezone_default_time_sessions
local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

# Define the DAG
with DAG(
    dag_display_name="[Securities] Daily Update Stock Price ",
    dag_id='daily_update_stock_price',
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    schedule="0 15 * * *",  # Daily at 15:00 (after market close)
    catchup=False,
) as dag:
    DAG_CONFIG_VAR_NAME = f"dag_config_{dag.dag_id}"
    dag_config = _load_dag_config(DAG_CONFIG_VAR_NAME)
    interval_defs = dag_config.get('intervals', [])

    download_task = PythonOperator(
        task_display_name="Download Stock Data",
        task_id="download_securities_data",
        python_callable=download_securities_data,
        op_kwargs={'dag_config': dag_config},
        retries=3,
        retry_delay=timedelta(minutes=5),
    )

    push_tasks = []
    for interval_def in interval_defs:
        minutes = int(interval_def.get('minutes'))
        table_name = interval_def.get('table_name')
        if table_name is None:
            raise ValueError("Each interval config must include 'table_name'.")

        interval_label = _to_interval_label(minutes)
        push_tasks.append(
            PythonOperator(
                task_display_name=f"Push {interval_label} Stock Data to ClickHouse",
                task_id=f"push_stock_{interval_label.lower()}_to_clickhouse",
                python_callable=push_to_clickhouse,
                op_kwargs={'interval_minutes': minutes, 'table_name': table_name, 'dag_config': dag_config},
                retries=3,
                retry_delay=timedelta(minutes=2),
            )
        )

    download_task >> push_tasks


# with DAG(
#     dag_display_name="[Securities] Daily Update Derivative Price ",
#     dag_id='daily_update_stock_price',
#     start_date=datetime(2025, 1, 1, tzinfo=local_tz),
#     schedule="0 15 * * *",  # Daily at 15:00 (after market close)
#     catchup=False,
# ) as dag:
#     DAG_CONFIG_VAR_NAME = f"{dag.dag_id}_config"
#     dag_config = _load_dag_config(DAG_CONFIG_VAR_NAME)
#     interval_defs = dag_config.get('intervals', [])
#
#     download_task = PythonOperator(
#         task_display_name="Download Derivative Data",
#         task_id="download_securities_data",
#         python_callable=download_securities_data,
#         op_kwargs={'dag_config': dag_config},
#         retries=3,
#         retry_delay=timedelta(minutes=5),
#     )
#
#     push_tasks = []
#     for interval_def in interval_defs:
#         minutes = int(interval_def.get('minutes'))
#         table_name = interval_def.get('table_name')
#         if table_name is None:
#             raise ValueError("Each interval config must include 'table_name'.")
#
#         interval_label = _to_interval_label(minutes)
#         push_tasks.append(
#             PythonOperator(
#                 task_display_name=f"Push {interval_label} Stock Data to ClickHouse",
#                 task_id=f"push_stock_{interval_label.lower()}_to_clickhouse",
#                 python_callable=push_to_clickhouse,
#                 op_kwargs={'interval_minutes': minutes, 'table_name': table_name, 'dag_config': dag_config},
#                 retries=3,
#                 retry_delay=timedelta(minutes=2),
#             )
#         )
#
#     download_task >> push_tasks