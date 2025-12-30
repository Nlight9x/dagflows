"""
DAG for daily securities event data update
- Task: Download event data from Vietstock

IMPORTANT: Before running this DAG, ensure the Airflow Variable is configured.
"""
from airflow.sdk import DAG
from airflow.sdk import Variable
from airflow.sdk import Param
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import os
import time
import pendulum

import pandas as pd
from utils.market_data_connector import VietstockConnector
from utils.storage_exporter import ClickHouseExporter

# Import shared helper functions from securities_daily_update_dag
from dags.securities_daily_update_dag import (
    _get_data_date,
    _get_data_range_date,
    _get_data_dir,
    _cleanup_old_shared_data,
    export_records_to_clickhouse,
)

DEFAULT_DAG_CONFIG = {
    "mode": "Daily",
    "shared_root": "/opt/airflow/shared",
    "shared_subdir": "stock_event",
    "back_days": 1,
    "data_dir_keep_days": 5,
    "download_wait_seconds": 2,
    "export_batch_size": 1000,
}


def _get_param_defaults_from_base_config(raw_config):
    """Get default values for params from Variable (only for specified fields)"""
    if raw_config is None:
        return {}
    try:
        cfg = json.loads(raw_config) if isinstance(raw_config, str) else raw_config
        if not isinstance(cfg, dict):
            return {}
        
        # Only return values for params that can be overridden (event-specific, no base_resolution)
        param_fields = ['data_date', 'back_days', 'download_wait_seconds', 'symbols']
        return {k: v for k, v in cfg.items() if k in param_fields}
    except Exception:
        return {}


def _load_dag_base_config(config_var_name):
    """Load and validate DAG config from Airflow Variable (event-specific validation)"""
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

    clickhouse_cfg = merged.get('clickhouse_connection_config')
    if not isinstance(clickhouse_cfg, dict):
        raise ValueError("DAG config must include 'clickhouse_connection_config' object with connection info.")
    for key in ["host", "port", "database", "user", "password"]:
        if key not in clickhouse_cfg:
            raise ValueError(f"ClickHouse config must include '{key}' field.")

    push_task_config = merged.get('push_task_config')
    if not isinstance(push_task_config, dict) or 'table_name' not in push_task_config:
        raise ValueError("DAG config must include 'push_task_config' object with 'table_name' field.")

    return merged


def _to_runtime_dag_config(base_config, **context):
    """
    Merge params from DAG params into config (only for specified fields).
    Params override Variable config values.
    Event-specific: no base_resolution param
    """
    # Get params from context (from DAG params)
    params = context.get('params', {})
    if not isinstance(params, dict):
        return base_config
    
    # Only merge specified param fields (event-specific, no base_resolution)
    param_fields = ['data_date', 'back_days', 'download_wait_seconds', 'symbols']
    merged_config = base_config.copy()
    
    for field in param_fields:
        if field in params:
            merged_config[field] = params[field]
            print(f"Using param override for '{field}': {params[field]}")
    
    return merged_config


def download_vnstock_event(dag_config, **context):
    """
    Download securities event data from Vietstock
    Data is saved to shared folder with standardized format
    """
    # Merge params into config (only for specified fields)
    dag_config = _to_runtime_dag_config(dag_config, **context)
    
    execution_date = _get_data_date(dag_config, **context).date()

    keep_days = int(dag_config.get('data_dir_keep_days', 5))
    wait_seconds = float(dag_config.get('download_wait_seconds', 2))

    data_dir = _get_data_dir(dag_config, execution_date)
    os.makedirs(data_dir, exist_ok=True)
    _cleanup_old_shared_data(dag_config, execution_date, keep_days=keep_days)

    symbols = dag_config.get('symbols')

    from_date, to_date, back_days = _get_data_range_date(dag_config, **context)
    date_str = to_date.strftime("%Y-%m-%d")

    print(f"Downloading securities event data.")
    print(f"Date: {date_str}")
    print(f"Output directory: {data_dir}")

    downloaded_files = []
    with VietstockConnector(timeout=30.0) as connector:
        for idx, symbol in enumerate(symbols):
            print(f"[{idx + 1}/{len(symbols)}] Downloading events for {symbol} from Vietstock...")

            try:
                df = connector.get_event(
                    symbol=symbol,
                    from_timestamp=int(from_date.timestamp()),
                    to_timestamp=int(to_date.timestamp()),
                    resolution='1d'  # Event data typically uses daily resolution
                )

                output_path = os.path.join(data_dir, f"{symbol}_event_{date_str}.csv")
                df.to_csv(output_path, index=False)

                record_count = len(df)
                print(f"[{idx + 1}/{len(symbols)}] Saved {symbol} events: {output_path} ({record_count} records)")

                downloaded_files.append({
                    'symbol': symbol, 'source': 'vietstock', 'file_path': output_path, 'record_count': record_count,
                    'status': 'success'})

                if idx < len(symbols) - 1 and wait_seconds > 0:
                    print(f"Waiting {wait_seconds} seconds before next request...")
                    time.sleep(wait_seconds)

            except Exception as e:
                print(f"Error downloading events for {symbol} from Vietstock: {e}")
                downloaded_files.append({
                    'symbol': symbol, 'source': 'vietstock', 'file_path': None, 'record_count': 0, 'status': 'failed',
                    'error': str(e)})

    metadata_path = os.path.join(data_dir, "event_metadata.json")
    metadata = {
        'data_date': date_str,
        'back_days': back_days,
        'data_type': 'event',
        'total_symbols': len(symbols),
        'downloaded_files': downloaded_files,
        'success_count': sum(1 for f in downloaded_files if f.get('status') == 'success'),
        'failed_count': sum(1 for f in downloaded_files if f.get('status') == 'failed')
    }

    with open(metadata_path, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)
        print(f"Metadata saved to: {metadata_path}")

    print(f"Successfully downloaded event data for {metadata['success_count']}/{len(symbols)} symbol(s)")
    return metadata


def push_event_to_clickhouse(table_name, dag_config, **context):
    """Push event data to ClickHouse using DAG configuration"""
    # Merge params into config (only for specified fields)
    dag_config = _to_runtime_dag_config(dag_config, **context)
    
    from_date, to_date, back_days = _get_data_range_date(dag_config, **context)

    clickhouse_config = dag_config.get('clickhouse_connection_config')
    if isinstance(clickhouse_config, str):
        clickhouse_config = json.loads(clickhouse_config)
    if not isinstance(clickhouse_config, dict) or not clickhouse_config:
        raise ValueError("ClickHouse configuration missing in DAG config variable")

    data_dir = _get_data_dir(dag_config, to_date)

    metadata_path = os.path.join(data_dir, "event_metadata.json") if data_dir else None
    if not metadata_path or not os.path.exists(metadata_path):
        raise FileNotFoundError(f"Metadata file not found: {metadata_path}. Please run download task first.")

    with open(metadata_path, 'r', encoding='utf-8') as f:
        metadata = json.load(f)

    print(f"Pushing event data to ClickHouse table: {table_name}")
    print(f"Data directory: {data_dir}")
    print(f"Found {len(metadata['downloaded_files'])} files to process")

    clickhouse_config['table_name'] = table_name
    exporter = ClickHouseExporter(**clickhouse_config)
    
    # Get all symbols from downloaded files
    symbols_to_delete = [
        f.get('symbol') for f in metadata['downloaded_files'] 
        if f.get('status') == 'success' and f.get('symbol')
    ]
    dates_to_delete = [from_date.date() + timedelta(days=i) for i in range(back_days + 1)]
    print("Delete old records.")
    exporter.delete_by_symbol_and_date(dates=dates_to_delete, symbols=symbols_to_delete)

    total_exported = 0
    results = []

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

            df = pd.read_csv(file_path, parse_dates=['datetime'])
            if df.empty or df['datetime'].isna().any():
                print(f"Skipping file due to invalid datetime values: {file_path}")
                continue

            if 'timestamp' not in df.columns:
                df['timestamp'] = (pd.to_datetime(df['datetime']).view('int64') // 10**9).astype(int)

            # Event data doesn't need aggregation, just ensure symbol and date columns exist
            if 'symbol' not in df.columns:
                df['symbol'] = symbol
            if 'date' not in df.columns:
                df['date'] = pd.to_datetime(df['datetime']).dt.date

            if df.empty:
                continue

            batch_size = int(dag_config.get('export_batch_size', 1000))
            result = export_records_to_clickhouse(exporter, df, batch_size=batch_size)
            exported_count = result.get('exported_records', 0)
            total_exported += exported_count
            print(f"Exported {exported_count} event records for {symbol} -> {table_name}")
            results.append({'symbol': symbol, 'exported_records': exported_count, 'status': 'success'})

        except Exception as e:
            print(f"Error processing event data for {symbol}: {e}")
            results.append({'symbol': symbol, 'exported_records': 0, 'status': 'failed', 'error': str(e)})

    summary = {
        'date': to_date.strftime("%Y-%m-%d"),
        'table': table_name,
        'total_exported': total_exported,
        'processed_symbols': len(results),
        'success_count': sum(1 for r in results if r.get('status') == 'success'),
        'failed_count': sum(1 for r in results if r.get('status') == 'failed'),
        'results': results
    }
    print(f"Summary: Exported {total_exported} event records to ClickHouse table '{table_name}'")
    return summary


def render_dag(dag_id, **config):
    _DAG_ID = dag_id
    _DAG_CONFIG_VAR_NAME = f"dag_config_{_DAG_ID}"
    _dag_config = _load_dag_base_config(_DAG_CONFIG_VAR_NAME)
    _param_defaults = _get_param_defaults_from_base_config(_dag_config)

    with DAG(
        dag_id=_DAG_ID,
        params={
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
        },
        **config
    ) as dag:
        download_task = PythonOperator(
            task_display_name="Download Event Data",
            task_id="download_vnstock_event",
            python_callable=download_vnstock_event,
            op_kwargs={'dag_config': _dag_config},
            retries=3,
            retry_delay=timedelta(minutes=5),
        )

        push_task_config = _dag_config.get('push_task_config', {})
        table_name = push_task_config.get('table_name')
        if table_name:
            push_task = PythonOperator(
                task_display_name="Push Event Data to ClickHouse",
                task_id="push_event_to_clickhouse",
                python_callable=push_event_to_clickhouse,
                op_kwargs={'table_name': table_name, 'dag_config': _dag_config},
                retries=3,
                retry_delay=timedelta(minutes=2),
            )
            download_task >> push_task

    return dag


# Set timezone
local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

dag_1 = render_dag(
    dag_id='update_hose_stock_events',
    dag_display_name="[Securities] Update HOSE Stock Event",
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    schedule="10 9 * * 1-5",  # Mon-Fri at 15:00 (after market close)
    catchup=False,
)

