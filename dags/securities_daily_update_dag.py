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
from datetime import datetime, timedelta
import pendulum
import json
import os
import time

from utils.market_data_connector import VietstockConnector
from utils.storage_exporter import ClickHouseExporter


def _get_data_date(**context):
    """Get the data date from context (previous day)"""
    logical_date = context.get('logical_date') if 'logical_date' in context else datetime.now()
    return (logical_date - timedelta(days=1)).date()


def download_securities_data(**context):
    """
    Download securities data from multiple connectors (Vietstock, future: other connectors)
    Data is saved to shared folder with standardized format
    """
    execution_date = _get_data_date(**context)
    date_str = execution_date.strftime("%Y-%m-%d")
    
    # Get list of symbols from Airflow Variable
    symbols_str = Variable.get("securities_symbols", default='["HPG", "VNM", "FPT"]')
    try:
        symbols = json.loads(symbols_str) if isinstance(symbols_str, str) else symbols_str
    except:
        symbols = ["HPG", "VNM", "FPT"]  # Default fallback
    
    if not isinstance(symbols, list):
        symbols = [symbols]
    
    resolution = "1"  # 1 minute data
    
    # Convert date to UNIX timestamp
    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
    from_timestamp = str(int(date_obj.timestamp()))
    to_timestamp = str(int((date_obj + timedelta(days=1)).timestamp()))
    
    # Create shared data directory
    shared_dir = "/opt/airflow/shared"
    data_dir = os.path.join(shared_dir, "securities", date_str)
    os.makedirs(data_dir, exist_ok=True)
    
    print(f"Downloading securities data for {len(symbols)} symbol(s): {', '.join(symbols)}")
    print(f"Date: {date_str}")
    print(f"Resolution: {resolution} (1 minute)")
    print(f"Output directory: {data_dir}")
    
    downloaded_files = []
    
    # Try VietstockConnector first
    vietstock_success = False
    try:
        with VietstockConnector(timeout=30.0) as connector:
            for idx, symbol in enumerate(symbols):
                print(f"[{idx + 1}/{len(symbols)}] Downloading {symbol} from Vietstock...")
                
                try:
                    # Fetch data
                    data = connector.get_history(
                        symbol=symbol,
                        resolution=resolution,
                        from_timestamp=from_timestamp,
                        to_timestamp=to_timestamp
                    )
                    
                    # Transform data to standardized format
                    # Vietstock API returns: {"t": [timestamps], "c": [closes], "o": [opens], "h": [highs], "l": [lows], "v": [volumes]}
                    standardized_data = []
                    
                    if isinstance(data, dict) and 't' in data:
                        timestamps = data.get('t', [])
                        opens = data.get('o', [])
                        highs = data.get('h', [])
                        lows = data.get('l', [])
                        closes = data.get('c', [])
                        volumes = data.get('v', [])
                        
                        for i, ts in enumerate(timestamps):
                            standardized_data.append({
                                'symbol': symbol,
                                'timestamp': ts,
                                'datetime': datetime.fromtimestamp(ts).isoformat(),
                                'open': opens[i] if i < len(opens) else None,
                                'high': highs[i] if i < len(highs) else None,
                                'low': lows[i] if i < len(lows) else None,
                                'close': closes[i] if i < len(closes) else None,
                                'volume': volumes[i] if i < len(volumes) else None,
                                'source': 'vietstock',
                                'date': date_str
                            })
                    else:
                        # If data format is different, try to use as is
                        print(f"Warning: Unexpected data format for {symbol}, using raw data")
                        if isinstance(data, list):
                            standardized_data = [
                                {**item, 'symbol': symbol, 'source': 'vietstock', 'date': date_str}
                                for item in data
                            ]
                    
                    # Save to JSON file with standardized format
                    output_filename = f"{symbol}_{date_str}.json"
                    output_path = os.path.join(data_dir, output_filename)
                    
                    with open(output_path, 'w', encoding='utf-8') as f:
                        json.dump(standardized_data, f, ensure_ascii=False, indent=2)
                    
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
                    
                    # Sleep between requests (except for the last one)
                    if idx < len(symbols) - 1:
                        print(f"Waiting 2 seconds before next request...")
                        time.sleep(2)
                        
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
            # Future: Try other connectors here
            print("Future: Could try other connectors (e.g., VNDirect, etc.)")
            raise
    
    # Save metadata file
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


def push_to_clickhouse(**context):
    """
    Push downloaded securities data to ClickHouse table
    Reads data from shared folder and inserts into ClickHouse
    """
    execution_date = _get_data_date(**context)
    date_str = execution_date.strftime("%Y-%m-%d")
    
    # Get ClickHouse connection info from Airflow Variables
    clickhouse_config = Variable.get("clickhouse_connection_info", default=None)
    if clickhouse_config:
        try:
            clickhouse_config = json.loads(clickhouse_config) if isinstance(clickhouse_config, str) else clickhouse_config
        except:
            clickhouse_config = None
    
    if not clickhouse_config:
        # Fallback to individual variables
        clickhouse_config = {
            'host': Variable.get("clickhouse_host", default='localhost'),
            'port': int(Variable.get("clickhouse_port", default='9000')),
            'database': Variable.get("clickhouse_database", default='default'),
            'user': Variable.get("clickhouse_user", default='default'),
            'password': Variable.get("clickhouse_password", default=''),
        }
    
    # Get table name
    table_name = Variable.get("clickhouse_securities_table", default='stock_price_minute')
    clickhouse_config['table_name'] = table_name
    
    # Read data from shared folder
    shared_dir = "/opt/airflow/shared"
    data_dir = os.path.join(shared_dir, "securities", date_str)
    
    if not os.path.exists(data_dir):
        raise FileNotFoundError(f"Data directory not found: {data_dir}. Please run download task first.")
    
    # Read metadata
    metadata_path = os.path.join(data_dir, "metadata.json")
    if os.path.exists(metadata_path):
        with open(metadata_path, 'r', encoding='utf-8') as f:
            metadata = json.load(f)
    else:
        # Try to find all JSON files in directory
        metadata = {'downloaded_files': []}
        for filename in os.listdir(data_dir):
            if filename.endswith('.json') and filename != 'metadata.json':
                symbol = filename.replace(f'_{date_str}.json', '')
                metadata['downloaded_files'].append({
                    'symbol': symbol,
                    'file_path': os.path.join(data_dir, filename),
                    'status': 'success'
                })
    
    print(f"Pushing securities data to ClickHouse table: {table_name}")
    print(f"Data directory: {data_dir}")
    print(f"Found {len(metadata['downloaded_files'])} files to process")
    
    # Initialize ClickHouse exporter
    # Note: Table must be created beforehand. See SQL schema below for reference.
    exporter = ClickHouseExporter(**clickhouse_config)
    
    try:
        total_exported = 0
        results = []
        
        # Process each file
        for file_info in metadata['downloaded_files']:
            if file_info.get('status') != 'success':
                print(f"Skipping {file_info.get('symbol')}: status = {file_info.get('status')}")
                continue
            
            file_path = file_info.get('file_path')
            if not file_path or not os.path.exists(file_path):
                print(f"File not found: {file_path}")
                continue
            
            symbol = file_info.get('symbol', 'UNKNOWN')
            print(f"Processing {symbol}...")
            
            try:
                # Read data from JSON file
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                if not data or len(data) == 0:
                    print(f"No data in file: {file_path}")
                    continue
                
                # Export to ClickHouse
                result = exporter.export(
                    data=data,
                    batch_size=1000,
                    column_mapping=None,  # Optional: map column names if needed
                    keys=None,  # Optional: specify which keys to include/exclude
                    keys_mode='include'  # Optional: 'include' or 'exclude'
                )
                
                exported_count = result.get('exported_records', 0)
                total_exported += exported_count
                
                print(f"Exported {exported_count} records for {symbol}")
                
                results.append({
                    'symbol': symbol,
                    'exported_records': exported_count,
                    'status': 'success'
                })
                
            except Exception as e:
                print(f"Error processing {symbol}: {e}")
                results.append({
                    'symbol': symbol,
                    'exported_records': 0,
                    'status': 'failed',
                    'error': str(e)
                })
        
        summary = {
            'date': date_str,
            'total_exported': total_exported,
            'processed_files': len(results),
            'success_count': sum(1 for r in results if r.get('status') == 'success'),
            'failed_count': sum(1 for r in results if r.get('status') == 'failed'),
            'results': results
        }
        
        print(f"Summary: Exported {total_exported} records to ClickHouse")
        print(f"Success: {summary['success_count']}, Failed: {summary['failed_count']}")
        
        return summary
    finally:
        # Close ClickHouse connection
        exporter.close()


# Set timezone
local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

# Define the DAG
with DAG(
    dag_display_name="Securities Daily Update",
    dag_id="securities_daily_update",
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    schedule="0 15 * * *",  # Daily at 3:00 PM (after market close)
    catchup=False,
    # tags=["securities", "clickhouse", "daily"],
) as dag:
    
    # Task 1: Download securities data
    download_task = PythonOperator(
        task_display_name="Download Securities Data",
        task_id="download_securities_data",
        python_callable=download_securities_data,
        retries=3,
        retry_delay=timedelta(minutes=5),
    )
    
    # Task 2: Push to ClickHouse
    push_clickhouse_task = PythonOperator(
        task_display_name="Push to ClickHouse",
        task_id="push_to_clickhouse",
        python_callable=push_to_clickhouse,
        retries=3,
        retry_delay=timedelta(minutes=2),
    )
    
    # Define task dependencies
    download_task >> push_clickhouse_task

