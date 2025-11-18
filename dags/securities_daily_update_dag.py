"""
DAG for daily securities data update
- Task 1: Download stock data from multiple sources (Vietstock, future connectors)
- Task 2: Push downloaded data to ClickHouse

IMPORTANT: Before running this DAG, create the ClickHouse table first.
See securities_table_schema.sql for the table schema.
"""
from abc import ABC
from collections.abc import MutableMapping
from typing import Any

from airflow.sdk import DAG
from airflow.sdk import Variable
from airflow.sdk import Param
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta, date
import dateutil.relativedelta
import pendulum
import json
import os
import time
import shutil
import pandas as pd


from utils.market_data_connector import VietstockConnector
from utils.storage_exporter import ClickHouseExporter


DEFAULT_DAG_CONFIG = {
    "mode": "Daily",
    "shared_root": "/opt/airflow/shared",
    "shared_subdir": "stock",
    "base_resolution": "1m",
    "back_days": 1,
    "data_dir_keep_days": 5,
    "download_wait_seconds": 2,
    "export_batch_size": 1000,
}

_resolution_convert_map = {
    "1m": 1, "5m": 5, "30m": 30, "1h": 60, "1d": 1440, "1w": 10080
}


def _get_data_date(dag_config, **context):
    """Get trading date from Airflow context"""
    data_date = dag_config.get('data_date', None)
    if data_date:
        return datetime.strptime(data_date, "%Y-%m-%d")
    logical_date = context.get('logical_date') if 'logical_date' in context else datetime.now()
    return logical_date


def _get_data_range_date(dag_config, **context):
    back_days = int(dag_config.get('back_days'))
    to_date = _get_data_date(dag_config, **context)
    from_date = to_date - timedelta(days=back_days)
    return from_date, to_date, back_days


def _get_data_dir(dag_config, data_date):
    shared_root = dag_config.get('shared_root')
    shared_subdir = dag_config.get('shared_subdir')
    date_str = data_date.strftime("%Y-%m-%d")
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


def _to_minute_resolution(m_resolution):
    rs = m_resolution.lower()
    if rs not in _resolution_convert_map:
        raise ValueError(f"Resolution '{m_resolution}' is invalid!")
    return _resolution_convert_map[rs]


def _get_param_defaults_from_base_config(raw_config):
    """Get default values for params from Variable (only for specified fields)"""
    if raw_config is None:
        return {}
    try:
        cfg = json.loads(raw_config) if isinstance(raw_config, str) else raw_config
        if not isinstance(cfg, dict):
            return {}
        
        # Only return values for params that can be overridden
        param_fields = ['base_resolution', 'data_date', 'back_days', 'download_wait_seconds', 'symbols']
        return {k: v for k, v in cfg.items() if k in param_fields}
    except Exception:
        return {}


def _load_dag_base_config(config_var_name):
    raw_config = Variable.get(config_var_name, default=None)
    if raw_config is None:
        raise ValueError(f"Airflow Variable '{config_var_name}' is not set. Please create it with DAG configuration.")

    cfg = json.loads(raw_config) if isinstance(raw_config, str) else raw_config
    if not isinstance(cfg, dict):
        raise ValueError(f"DAG config must be a JSON object. Got: {type(cfg)}")

    merged = DEFAULT_DAG_CONFIG.copy()
    merged.update(cfg)

    symbols = merged.get('symbols')
    if not isinstance(symbols, list) or len(symbols) == 0:
        raise ValueError("DAG config must include non-empty 'symbols' list.")

    base_resolution = merged.get('base_resolution')
    if base_resolution.lower() not in _resolution_convert_map:
        raise ValueError(f"Resolution '{base_resolution}' is invalid!")

    clickhouse_cfg = merged.get('clickhouse_connection_config')
    if not isinstance(clickhouse_cfg, dict):
        raise ValueError("DAG config must include 'clickhouse_connection_config' object with connection info.")
    for key in ["host", "port", "database", "user", "password"]:
        if key not in clickhouse_cfg:
            raise ValueError(f"ClickHouse config must include '{key}' field.")

    config_tasks = merged.get('push_config_tasks')
    if not isinstance(config_tasks, list) or len(config_tasks) == 0:
        raise ValueError("DAG config must include 'push_config_tasks' list.")
    for cfg in config_tasks:
        if not isinstance(cfg, dict):
            raise ValueError("Each interval definition must be an object with 'minutes' and 'table_name'.")
        if 'resolution' not in cfg or 'table_name' not in cfg:
            raise ValueError("Interval definition missing 'minutes' or 'table_name'.")

    return merged


def _to_runtime_dag_config(base_config, **context):
    """
    Merge params from DAG params into config (only for specified fields).
    Params override Variable config values.
    """
    # Get params from context (from DAG params)
    params = context.get('params', {})
    if not isinstance(params, dict):
        return base_config
    
    # Only merge specified param fields
    param_fields = ['base_resolution', 'data_date', 'back_days', 'download_wait_seconds', 'symbols']
    merged_config = base_config.copy()
    
    for field in param_fields:
        if field in params:
            merged_config[field] = params[field]
            print(f"Using param override for '{field}': {params[field]}")
    
    return merged_config


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
    for date_dir in os.listdir(base_dir):
        dir_path = os.path.join(base_dir, date_dir)
        if not os.path.isdir(dir_path):
            continue
        try:
            dir_date = datetime.strptime(date_dir, '%Y-%m-%d').date()
        except ValueError:
            continue
        if dir_date < cutoff_date:
            shutil.rmtree(dir_path, ignore_errors=True)
            print(f"Removed old data directory: {dir_path}")


def _get_symbol_ticket_of_vn30d(due_month=0):
    now = datetime.now()
    # Tìm ngày thứ 5 lần thứ 3 trong tháng hiện tại
    first_day = now.replace(day=1)
    weekday_count = 0
    expiry_day = None
    for i in range(31):
        d = first_day + timedelta(days=i)
        if d.month != now.month:
            break
        if d.weekday() == 3:  # Thứ 5 (Monday=0)
            weekday_count += 1
            if weekday_count == 3:
                expiry_day = d
                break
    # Nếu đã qua ngày đáo hạn, chuyển sang tháng sau
    if now.date() > expiry_day.date():
        base_month = now + dateutil.relativedelta.relativedelta(months=1)
    else:
        base_month = now
    # Cộng thêm due_month nếu có
    base_month = base_month + dateutil.relativedelta.relativedelta(months=due_month)
    year = base_month.year
    month = base_month.month
    # Map năm sang ký tự quy ước
    year_map = "ABCDEFGHIJKLMNPQRSTVWXYZ0123456789"
    base_year = 2020
    year_idx = (year - base_year) % len(year_map)
    year_code = year_map[year_idx]
    # Map tháng sang ký tự quy ước (1-9, A, B, C)
    if 1 <= month <= 9:
        month_code = str(month)
    else:
        month_code = chr(ord('A') + (month - 10))  # 10->A, 11->B, 12->C
    symbol = f"41I1{year_code}{month_code}000"
    return symbol


def download_securities_data(dag_config, **context):
    """
    Download securities data from multiple connectors (Vietstock, future: other connectors)
    Data is saved to shared folder with standardized format
    """
    # Merge params into config (only for specified fields)
    dag_config = _to_runtime_dag_config(dag_config, **context)
    
    execution_date = _get_data_date(dag_config, **context)
    # date_str = execution_date.strftime("%Y-%m-%d")

    keep_days = int(dag_config.get('data_dir_keep_days', 5))
    wait_seconds = float(dag_config.get('download_wait_seconds', 2))

    data_dir = _get_data_dir(dag_config, execution_date)
    os.makedirs(data_dir, exist_ok=True)
    _cleanup_old_shared_data(dag_config, execution_date, keep_days=keep_days)

    symbols = dag_config.get('symbols')
    base_resolution = dag_config.get("base_resolution")

    from_date, to_date, back_days = _get_data_range_date(dag_config, **context)
    date_str = to_date.strftime("%Y-%m-%d")

    print(f"Downloading securities data for {len(symbols)} symbol(s): {', '.join(symbols)}")
    print(f"Date: {date_str}")
    print(f"Resolution: {base_resolution}")
    print(f"Output directory: {data_dir}")

    downloaded_files = []
    with VietstockConnector(timeout=30.0) as connector:
        for idx, symbol in enumerate(symbols):
            print(f"[{idx + 1}/{len(symbols)}] Downloading {symbol} from Vietstock...")

            try:
                df = connector.get_history(
                    symbol=symbol, resolution=base_resolution,
                    from_timestamp=int(from_date.timestamp()), to_timestamp=int(to_date.timestamp()))

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
        'data_date': date_str,
        'back_days': back_days,
        'resolution': base_resolution,
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


def aggregate_minute_records(records: pd.DataFrame, minute_resolution):
    if records.empty or not minute_resolution or minute_resolution <= 1:
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

    resampled = df.resample(f'{minute_resolution}T').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    })

    resampled.reset_index(inplace=True)
    resampled['timestamp'] = (resampled['datetime'].view('int64') // 10**9).astype(int)

    return resampled[['datetime', 'timestamp', 'open', 'high', 'low', 'close', 'volume']]


def export_records_to_clickhouse(exporter, records_df: pd.DataFrame, batch_size=1000):
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


def push_to_clickhouse(resolution, table_name, dag_config, **context):
    """Push securities data to ClickHouse using DAG configuration"""
    # Merge params into config (only for specified fields)
    dag_config = _to_runtime_dag_config(dag_config, **context)
    
    from_date, to_date, back_days = _get_data_range_date(dag_config, **context)

    clickhouse_config = dag_config.get('clickhouse')
    if isinstance(clickhouse_config, str):
        clickhouse_config = json.loads(clickhouse_config)
    if not isinstance(clickhouse_config, dict) or not clickhouse_config:
        raise ValueError("ClickHouse configuration missing in DAG config variable")

    data_dir = _get_data_dir(dag_config, to_date)

    metadata_path = os.path.join(data_dir, "metadata.json") if data_dir else None
    if not metadata_path or not os.path.exists(metadata_path):
        raise FileNotFoundError(f"Metadata file not found: {metadata_path}. Please run download task first.")

    with open(metadata_path, 'r', encoding='utf-8') as f:
        metadata = json.load(f)

    minute_resolution = _to_minute_resolution(resolution)
    downloaded_resolution = metadata.get('resolution')
    total_exported = 0
    results = []

    if minute_resolution % downloaded_resolution == 0:
        print(f"Pushing {resolution} data to ClickHouse table: {table_name}")
        print(f"Data directory: {data_dir}")
        print(f"Found {len(metadata['downloaded_files'])} files to process")

        clickhouse_config['table_name'] = table_name
        exporter = ClickHouseExporter(**clickhouse_config)
        exporter.delete_by_dates([from_date.date() + timedelta(days=i) for i in range(back_days + 1)])

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

                records_df = aggregate_minute_records(df, minute_resolution)
                if records_df.empty:
                    continue
                records_df['symbol'] = symbol
                records_df['date'] = records_df['datetime'].dt.date

                if records_df.empty:
                    continue

                # print(records_df)
                batch_size = int(dag_config.get('export_batch_size', 1000))
                result = export_records_to_clickhouse(exporter, records_df, batch_size=batch_size)
                exported_count = result.get('exported_records', 0)
                total_exported += exported_count
                print(f"Exported {exported_count} {resolution} records for {symbol} -> {table_name}")
                results.append({'symbol': symbol, 'exported_records': exported_count, 'status': 'success'})

            except Exception as e:
                print(f"Error processing {resolution} data for {symbol}: {e}")
                results.append({'symbol': symbol, 'exported_records': 0, 'status': 'failed', 'error': str(e)})
    else:
        print(f"Skip processing {resolution} data.")

    summary = {
        'date': to_date.strftime("%Y-%m-%d"),
        'resolution': resolution,
        'table': table_name,
        'total_exported': total_exported,
        'processed_symbols': len(results),
        'success_count': sum(1 for r in results if r.get('status') == 'success'),
        'failed_count': sum(1 for r in results if r.get('status') == 'failed'),
        'results': results
    }
    print(f"Summary: Exported {total_exported} records to ClickHouse table '{table_name}'")
    return summary


# Set timezone_default_time_sessions
local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

# Define the DAG
# Load param defaults from Variable before DAG definition
_DAG_ID = 'daily_update_stock_price'
_DAG_CONFIG_VAR_NAME = f"dag_config_{_DAG_ID}"
_dag_config = _load_dag_base_config(_DAG_CONFIG_VAR_NAME)
_param_defaults = _get_param_defaults_from_base_config(_dag_config)


# class ParamsDict(MutableMapping[str, Any], ABC):
#     def __init__(self, params: dict):
#         pass


with DAG(
    dag_display_name="[Securities] Daily Update Stock Price ",
    dag_id=_DAG_ID,
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    schedule="0 15 * * 1-5",  # Mon-Fri at 15:00 (after market close)
    catchup=False,
    params={
        "base_resolution": Param(
            default=_param_defaults.get("base_resolution", "1m"),
            type="string",
            description="Base resolution for data download (e.g., 1m, 5m, 1d)"
        ),
        "data_date": Param(
            default=_param_defaults.get("data_date"),
            type=["string", "null"],
            format='date',
            description="Specific date to download (YYYY-MM-DD). If null, uses execution date"
        ),
        "back_days": Param(
            default=_param_defaults.get("back_days", 1),
            type="integer",
            description="Number of days to go back from data_date"
        ),
        "download_wait_seconds": Param(
            default=_param_defaults.get("download_wait_seconds", 2),
            type="number",
            description="Wait time between downloads (seconds)"
        ),
        "symbols": Param(
            default=_param_defaults.get("symbols", []),
            type="array",
            description="List of stock symbols to download"
        ),
    }
) as dag:
    # DAG_CONFIG_VAR_NAME = f"dag_config_{dag.dag_id}"
    # d_config = _load_dag_config(DAG_CONFIG_VAR_NAME)
    push_tasks_configs = _dag_config.get('push_tasks_configs', [])

    download_task = PythonOperator(
        task_display_name="Download Stock Data",
        task_id="download_securities_data",
        python_callable=download_securities_data,
        op_kwargs={'dag_config': _dag_config},
        retries=3,
        retry_delay=timedelta(minutes=5),
    )

    push_tasks = []
    for c_task in push_tasks_configs:
        resolution = c_task.get('resolution')
        table_name = c_task.get('table_name')
        if table_name is None:
            raise ValueError("Each interval config must include 'table_name'.")
        push_tasks.append(
            PythonOperator(
                task_display_name=f"Push {resolution} Stock Data to ClickHouse",
                task_id=f"push_stock_{resolution.lower()}_to_clickhouse",
                python_callable=push_to_clickhouse,
                op_kwargs={'resolution': resolution, 'table_name': table_name, 'dag_config': _dag_config},
                retries=3,
                retry_delay=timedelta(minutes=2),
            )
        )

    download_task >> push_tasks


# with DAG(
#     dag_display_name="[Securities] Daily Update VN30 Derivative Index",
#     dag_id='daily_update_vn30d_index',
#     start_date=datetime(2025, 1, 1, tzinfo=local_tz),
#     schedule="30 15 * * 1-5",  # Daily at 15:00 (after market close)
#     catchup=False,
# ) as dag:
#     DAG_CONFIG_VAR_NAME = f"{dag.dag_id}_config"
#     dag_config = _load_dag_config(DAG_CONFIG_VAR_NAME)
#     interval_defs = dag_config.get('intervals', [])
#
#     download_task = PythonOperator(
#         task_display_name="Download VN30 Derivative Data",
#         task_id="download_vn30d_data",
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
#                 task_display_name=f"Push {interval_label} VN30 Derivative Data to ClickHouse",
#                 task_id=f"push_vn30d_{interval_label.lower()}_to_clickhouse",
#                 python_callable=push_to_clickhouse,
#                 op_kwargs={'interval_minutes': minutes, 'table_name': table_name, 'dag_config': dag_config},
#                 retries=3,
#                 retry_delay=timedelta(minutes=2),
#             )
#         )
#
#     download_task >> push_tasks