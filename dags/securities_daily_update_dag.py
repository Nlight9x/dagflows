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
from datetime import datetime, timedelta, date
import pendulum
import json
import os
import time
import shutil

from utils.market_data_connector import VietstockConnector
from utils.storage_exporter import ClickHouseExporter

DAG_ID = "securities_daily_update"
DAG_CONFIG_VAR_NAME = f"{DAG_ID}_config"

DEFAULT_DAG_CONFIG = {
    "shared_root": "/opt/airflow/shared",
    "shared_subdir": "stock",
    "keep_days": 5,
    "download_wait_seconds": 2,
    "export_batch_size": 1000,
}


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle datetime and date objects"""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, date):
            return obj.isoformat()
        return super().default(obj)


def _get_data_date(**context):
    """Get the data date from context (previous day)"""
    logical_date = context.get('logical_date') if 'logical_date' in context else datetime.now()
    return (logical_date - timedelta(days=1)).date()


def load_dag_config():
    raw_config = Variable.get(DAG_CONFIG_VAR_NAME, default=None)
    if raw_config is None:
        raise ValueError(f"Airflow Variable '{DAG_CONFIG_VAR_NAME}' is not set. Please create it with DAG configuration.")
    try:
        cfg = json.loads(raw_config) if isinstance(raw_config, str) else raw_config
    except Exception as exc:
        raise ValueError(f"Failed to parse DAG config from Variable '{DAG_CONFIG_VAR_NAME}': {exc}")

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


def cleanup_old_shared_data(shared_root, shared_subdir, current_date, keep_days=5):
    """Remove data directories older than keep_days"""
    if keep_days <= 0:
        return
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

    shared_root = dag_config.get('shared_root')
    shared_subdir = dag_config.get('shared_subdir')
    if not shared_root or not shared_subdir:
        raise ValueError("DAG config must include 'shared_root' and 'shared_subdir'.")

    symbols = dag_config.get('symbols')
    if not isinstance(symbols, list) or len(symbols) == 0:
        raise ValueError("DAG config must include non-empty 'symbols' list.")

    keep_days = int(dag_config.get('keep_days', 5))
    wait_seconds = float(dag_config.get('download_wait_seconds', 2))

    shared_dir = os.path.join(shared_root, shared_subdir)
    data_dir = os.path.join(shared_dir, date_str)
    os.makedirs(shared_dir, exist_ok=True)
    os.makedirs(data_dir, exist_ok=True)
    cleanup_old_shared_data(shared_root, shared_subdir, execution_date, keep_days=keep_days)

    resolution = "1"

    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
    from_timestamp = str(int(date_obj.timestamp()))
    to_timestamp = str(int((date_obj + timedelta(days=1)).timestamp()))

    print(f"Downloading securities data for {len(symbols)} symbol(s): {', '.join(symbols)}")
    print(f"Date: {date_str}")
    print(f"Resolution: {resolution} (1 minute)")
    print(f"Output directory: {data_dir}")

    downloaded_files = []
    vietstock_success = False
    try:
        with VietstockConnector(timeout=30.0) as connector:
            for idx, symbol in enumerate(symbols):
                print(f"[{idx + 1}/{len(symbols)}] Downloading {symbol} from Vietstock...")

                try:
                    data = connector.get_history(
                        symbol=symbol,
                        resolution=resolution,
                        from_timestamp=from_timestamp,
                        to_timestamp=to_timestamp
                    )

                    standardized_data = []

                    if isinstance(data, dict) and 't' in data:
                        timestamps = data.get('t', [])
                        opens = data.get('o', [])
                        highs = data.get('h', [])
                        lows = data.get('l', [])
                        closes = data.get('c', [])
                        volumes = data.get('v', [])

                        for i, ts in enumerate(timestamps):
                            dt = datetime.fromtimestamp(ts)
                            standardized_data.append({
                                'symbol': symbol,
                                'timestamp': ts,
                                'datetime': dt,
                                'open': opens[i] if i < len(opens) else None,
                                'high': highs[i] if i < len(highs) else None,
                                'low': lows[i] if i < len(lows) else None,
                                'close': closes[i] if i < len(closes) else None,
                                'volume': volumes[i] if i < len(volumes) else None,
                                'date': dt.date()
                            })
                    elif isinstance(data, list):
                        day_value = datetime.strptime(date_str, '%Y-%m-%d').date()
                        for item in data:
                            standardized_data.append({**item, 'symbol': symbol, 'date': day_value})
                    else:
                        print(f"Warning: Unexpected data format for {symbol}, skipping")
                        standardized_data = []

                    output_filename = f"{symbol}_{date_str}.json"
                    output_path = os.path.join(data_dir, output_filename)

                    with open(output_path, 'w', encoding='utf-8') as f:
                        json.dump(standardized_data, f, ensure_ascii=False, indent=2, cls=DateTimeEncoder)

                    record_count = len(standardized_data)
                    print(f"[{idx + 1}/{len(symbols)}] Saved {symbol}: {output_path} ({record_count} records)")

                    downloaded_files.append({
                        'symbol': symbol,
                        'source': 'vietstock',
                        'file_path': output_path,
                        'record_count': record_count,
                        'status': 'success'
                    })

                    vietstock_success = True

                    if idx < len(symbols) - 1 and wait_seconds > 0:
                        print(f"Waiting {wait_seconds} seconds before next request...")
                        time.sleep(wait_seconds)

                except Exception as e:
                    print(f"Error downloading {symbol} from Vietstock: {e}")
                    downloaded_files.append({
                        'symbol': symbol,
                        'source': 'vietstock',
                        'file_path': None,
                        'record_count': 0,
                        'status': 'failed',
                        'error': str(e)
                    })

    except Exception as e:
        print(f"VietstockConnector failed: {e}")
        if not vietstock_success:
            raise

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

    print(f"Successfully downloaded data for {metadata['success_count']}/{len(symbols)} symbol(s)")
    print(f"Metadata saved to: {metadata_path}")

    return metadata


def aggregate_minute_records(records, interval_minutes):
    """
    Aggregate 1-minute OHLCV records to N-minute bars.
    - records: list of dicts with keys: timestamp, open, high, low, close, volume
    - interval_minutes: int (e.g., 5, 15, 60)
    Returns list of tuples (window_start_ts, agg_dict)
    """
    if not records:
        return []
    window_sec = int(interval_minutes) * 60
    windows = {}
    for r in records:
        ts = int(r.get('timestamp'))
        win_start = (ts // window_sec) * window_sec
        if win_start not in windows:
            windows[win_start] = {
                'open': r.get('open'),
                'high': r.get('high'),
                'low': r.get('low'),
                'close': r.get('close'),
                'volume': r.get('volume') or 0,
                '_first_ts': ts,
                '_last_ts': ts
            }
        else:
            w = windows[win_start]
            if ts < w['_first_ts']:
                w['_first_ts'] = ts
                w['open'] = r.get('open')
            if ts >= w['_last_ts']:
                w['_last_ts'] = ts
                w['close'] = r.get('close')
            cur_high = r.get('high')
            cur_low = r.get('low')
            if cur_high is not None and (w['high'] is None or cur_high > w['high']):
                w['high'] = cur_high
            if cur_low is not None and (w['low'] is None or cur_low < w['low']):
                w['low'] = cur_low
            vol = r.get('volume') or 0
            try:
                w['volume'] = (w['volume'] or 0) + (vol or 0)
            except:
                pass
    return [(k, windows[k]) for k in sorted(windows.keys())]


def export_records_to_clickhouse(records, table_name, clickhouse_config, batch_size=1000):
    """Export a list of dict records to ClickHouse table using ClickHouseExporter"""
    cfg = dict(clickhouse_config)
    cfg['table_name'] = table_name
    exporter = ClickHouseExporter(**cfg)
    try:
        return exporter.export(
            data=records,
            batch_size=batch_size,
            column_mapping=None,
            keys=None,
            keys_mode='include'
        )
    finally:
        exporter.close()


def parse_records_types(records):
    """
    Normalize types for records loaded from JSON:
    - Convert 'datetime' strings to datetime objects (fallback to 'timestamp')
    - Convert 'date' strings to date objects (fallback to from 'datetime')
    Mutates the input list in place and also returns it for convenience.
    """
    if not records:
        return records
    for rec in records:
        # datetime
        if 'datetime' in rec and isinstance(rec['datetime'], str):
            try:
                rec['datetime'] = datetime.fromisoformat(rec['datetime'].replace('Z', '+00:00'))
            except:
                try:
                    rec['datetime'] = datetime.strptime(rec['datetime'], '%Y-%m-%dT%H:%M:%S')
                except:
                    if 'timestamp' in rec:
                        rec['datetime'] = datetime.fromtimestamp(rec['timestamp'])
        # date
        if 'date' in rec and isinstance(rec['date'], str):
            try:
                rec['date'] = datetime.strptime(rec['date'], '%Y-%m-%d').date()
            except:
                if 'datetime' in rec and isinstance(rec['datetime'], datetime):
                    rec['date'] = rec['datetime'].date()
    return records


def push_to_clickhouse(interval_minutes, table_name, dag_config, **context):
    """Push securities data to ClickHouse using DAG configuration"""
    execution_date = _get_data_date(**context)
    date_str = execution_date.strftime("%Y-%m-%d")

    original_interval = interval_minutes
    effective_interval = None
    if interval_minutes is not None:
        interval_minutes = int(interval_minutes)
        if interval_minutes <= 1:
            interval_desc = "1m"
        else:
            effective_interval = interval_minutes
            if interval_minutes % 1440 == 0:
                interval_desc = f"{interval_minutes // 1440}d"
            elif interval_minutes % 60 == 0:
                interval_desc = f"{interval_minutes // 60}h"
            else:
                interval_desc = f"{interval_minutes}m"
    else:
        interval_desc = "1m"

    clickhouse_config = dag_config.get('clickhouse')
    if isinstance(clickhouse_config, str):
        clickhouse_config = json.loads(clickhouse_config)
    if not isinstance(clickhouse_config, dict) or not clickhouse_config:
        raise ValueError("ClickHouse configuration missing in DAG config variable")

    shared_root = dag_config.get('shared_root', '/opt/airflow/shared')
    shared_subdir = dag_config.get('shared_subdir', 'stock')
    data_dir = os.path.join(shared_root, shared_subdir, date_str)

    if not os.path.exists(data_dir):
        raise FileNotFoundError(f"Data directory not found: {data_dir}. Please run download task first.")

    metadata_path = os.path.join(data_dir, "metadata.json")
    if os.path.exists(metadata_path):
        with open(metadata_path, 'r', encoding='utf-8') as f:
            metadata = json.load(f)
    else:
        metadata = {'downloaded_files': []}
        for filename in os.listdir(data_dir):
            if filename.endswith('.json') and filename != 'metadata.json':
                symbol = filename.replace(f'_{date_str}.json', '')
                metadata['downloaded_files'].append({
                    'symbol': symbol,
                    'file_path': os.path.join(data_dir, filename),
                    'status': 'success'
                })

    print(f"Pushing {interval_desc} data to ClickHouse table: {table_name}")
    print(f"Data directory: {data_dir}")
    print(f"Found {len(metadata['downloaded_files'])} files to process")

    total_exported = 0
    results = []
    date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()

    for file_info in metadata['downloaded_files']:
        if file_info.get('status') != 'success':
            continue
        file_path = file_info.get('file_path')
        if not file_path or not os.path.exists(file_path):
            continue
        symbol = file_info.get('symbol', 'UNKNOWN')

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if not data:
                continue

            parse_records_types(data)

            if effective_interval and effective_interval > 0:
                windows = aggregate_minute_records(data, effective_interval)
                records_to_export = [{
                    'symbol': symbol,
                    'timestamp': win_start,
                    'datetime': datetime.fromtimestamp(win_start),
                    'open': w.get('open'),
                    'high': w.get('high'),
                    'low': w.get('low'),
                    'close': w.get('close'),
                    'volume': w.get('volume'),
                    'date': date_obj
                } for win_start, w in windows]
            else:
                for r in data:
                    r.pop('source')
                records_to_export = data

            if not records_to_export:
                continue

            batch_size = int(dag_config.get('export_batch_size', 1000))
            result = export_records_to_clickhouse(records_to_export, table_name, clickhouse_config, batch_size=batch_size)
            exported_count = result.get('exported_records', 0)
            total_exported += exported_count
            print(f"Exported {exported_count} {interval_desc} records for {symbol} -> {table_name}")
            results.append({'symbol': symbol, 'exported_records': exported_count, 'status': 'success'})

        except Exception as e:
            print(f"Error processing {interval_desc} data for {symbol}: {e}")
            results.append({'symbol': symbol, 'exported_records': 0, 'status': 'failed', 'error': str(e)})

    summary = {
        'date': date_str,
        'interval_minutes': original_interval,
        'table': table_name,
        'total_exported': total_exported,
        'processed_symbols': len(results),
        'success_count': sum(1 for r in results if r.get('status') == 'success'),
        'failed_count': sum(1 for r in results if r.get('status') == 'failed'),
        'results': results
    }
    print(f"{interval_desc} Summary: Exported {total_exported} records to ClickHouse table '{table_name}'")
    return summary

# Set timezone
local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

# Define the DAG
with DAG(
    dag_display_name="Securities Daily Update",
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    schedule="0 15 * * *",  # Daily at 3:00 PM (after market close)
    catchup=False,
    # tags=["securities", "clickhouse", "daily"],
) as dag:
    dag_config = load_dag_config()
    interval_defs = dag_config.get('intervals', [])

    download_task = PythonOperator(
        task_display_name="Download Securities Data",
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

        if minutes <= 1:
            interval_label = "1m"
        elif minutes % 1440 == 0:
            interval_label = f"{minutes // 1440}d"
        elif minutes % 60 == 0:
            interval_label = f"{minutes // 60}h"
        else:
            interval_label = f"{minutes}m"

        task = PythonOperator(
            task_display_name=f"Push {interval_label} Stock Data to ClickHouse",
            task_id=f"push_stock_{interval_label.lower()}_to_clickhouse",
            python_callable=push_to_clickhouse,
            op_kwargs={
                'interval_minutes': minutes,
                'table_name': table_name,
                'dag_config': dag_config,
            },
            retries=3,
            retry_delay=timedelta(minutes=2),
        )
        push_tasks.append(task)

    download_task >> push_tasks

