from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import asyncio
import os

from utils.affiliate_connector import InvolveAsyncConnector
from utils.exporter import export_involve_conversion_to_csv

def download_affiliate_data(**context):
    secret_key = Variable.get("affiliate_secret_key")
    output_path = os.path.join(os.path.dirname(__file__), "../data/affiliate_data.csv")
    state_key = "affiliate_downloaded_once"

    # Kiểm tra trạng thái đã từng chạy chưa
    downloaded_once = Variable.get(state_key, default_var="0") == "1"

    async def fetch_and_save():
        connector = InvolveAsyncConnector(secret_key=secret_key)
        await connector.authenticate()
        all_data = []
        page = 1
        limit = 100
        if not downloaded_once:
            # Lần đầu: lấy toàn bộ
            while True:
                data, has_next = await connector.get_conversion(page=str(page), limit=str(limit))
                all_data.extend(data)
                if not has_next:
                    break
                page += 1
            Variable.set(state_key, "1")
        else:
            # Các lần sau: chỉ lấy ngày hôm qua
            yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
            while True:
                data, has_next = await connector.get_conversion(start_date=yesterday, end_date=yesterday, page=str(page), limit=str(limit))
                all_data.extend(data)
                if not has_next:
                    break
                page += 1
        export_involve_conversion_to_csv(output_path, all_data)

    asyncio.run(fetch_and_save())

with DAG(
    dag_id="affiliate_download_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["affiliate"],
) as dag:
    download_task = PythonOperator(
        task_id="download_affiliate_data",
        python_callable=download_affiliate_data,
        provide_context=True,
    ) 

with DAG(
    dag_id="affiliate_download_dag_2",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["affiliate"],
) as dag:
    download_task = PythonOperator(
        task_id="download_affiliate_data",
        python_callable=download_affiliate_data,
        provide_context=True,
    ) 