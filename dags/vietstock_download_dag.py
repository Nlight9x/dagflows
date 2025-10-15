from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.models.param import Param
from datetime import datetime, timedelta
import pendulum
import json
import os
import time

from utils.market_data_connector import VietstockConnector


def download_vietstock_data(**context):
    """
    Download stock data from Vietstock API
    Parameters are passed through dag_run params when triggering manually
    """
    # Get parameters from context
    params = context['params']
    
    # Get parameters
    symbols = params['symbols']
    resolution = params['resolution']
    from_date = params['from']
    to_date = params['to']
    
    # Convert date (YYYY-MM-DD) to UNIX timestamp
    from_timestamp = str(int(datetime.strptime(from_date, '%Y-%m-%d').timestamp()))
    to_timestamp = str(int(datetime.strptime(to_date, '%Y-%m-%d').timestamp()))
    
    print(f"Downloading Vietstock data for {len(symbols)} symbol(s): {', '.join(symbols)}")
    print(f"Resolution: {resolution}")
    print(f"Date range: {from_date} to {to_date} (timestamp: {from_timestamp} to {to_timestamp})")
    
    # Create data directory if it doesn't exist
    data_dir = os.path.join(os.path.dirname(__file__), "../data")
    os.makedirs(data_dir, exist_ok=True)
    
    results = []
    
    # Use VietstockConnector to fetch data for each symbol
    with VietstockConnector(timeout=30.0) as connector:
        for idx, symbol in enumerate(symbols):
            print(f"[{idx + 1}/{len(symbols)}] Processing symbol: {symbol}")
            
            # Fetch data
            data = connector.get_history(
                symbol=symbol, resolution=resolution, from_timestamp=from_timestamp, to_timestamp=to_timestamp
            )
            
            # Save to JSON file
            output_filename = f"vietstock_{symbol}_{from_date}_{to_date}.json"
            output_path = os.path.join(data_dir, output_filename)
            
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            record_count = len(data) if isinstance(data, list) else 'N/A'
            print(f"[{idx + 1}/{len(symbols)}] Saved {symbol}: {output_path} ({record_count} records)")
            
            results.append({
                'symbol': symbol,
                'output_file': output_path,
                'record_count': record_count
            })
            
            # Sleep 60 seconds between requests (except for the last one)
            if idx < len(symbols) - 1:
                print(f"Waiting 60 seconds before next request...")
                time.sleep(60)
    
    print(f"Successfully downloaded data for {len(results)} symbol(s)")
    
    return {
        'status': 'success',
        'total_symbols': len(results),
        'results': results
    }


# Set timezone
local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

# Define the DAG
with DAG(
    dag_display_name="[Vietstock] - Download Stock Data",
    dag_id="vietstock_download_stock_data",
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=["vietstock", "manual"],
    params={
        "symbols": Param( default=["HPG", "VNM", "FPT"], type="array", description="List of stock symbols to download"),
        "resolution": Param( default="1", type="string", description="Time resolution (1 for 1 day)" ),
        "from": Param( default="2024-01-01", type="string", format="date", description="Start date (YYYY-MM-DD)"),
        "to": Param( default="2025-05-31", type="string", format="date", description="End date (YYYY-MM-DD)" )
    }
) as dag:
    download_task = PythonOperator(
        task_display_name="Download Vietstock Data",
        task_id="download_vietstock_data",
        python_callable=download_vietstock_data,
        provide_context=True,
        retries=3,
        retry_delay=timedelta(minutes=1),
    )

