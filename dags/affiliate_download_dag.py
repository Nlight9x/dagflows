from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import asyncio
import os

from utils.affiliate_connector import InvolveAsyncConnector
from utils.storage_exporter import CsvExporter
from utils.storage_connector import NocodbConnector

def download_and_export_csv_affiliate_data(**context):
    secret_key = Variable.get("affiliate_secret_key")
    output_path = os.path.join(os.path.dirname(__file__), "../data/affiliate_data.csv")
    state_key = "affiliate_downloaded_once"

    # Kiểm tra trạng thái đã từng chạy chưa
    downloaded_once = Variable.get(state_key, default_var="0") == "1"

    async def fetch_all_data():
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
        return all_data

    def export_csv(data):
        CsvExporter(filepath=output_path).export(data)

    all_data = asyncio.run(fetch_all_data())
    export_csv(all_data)

def download_and_export_nocodb_affiliate_data(**context):
    secret_key = Variable.get("involve_asisa_secret_key_1")
    nocodb_api_url = Variable.get("nocodb_api_url")
    nocodb_token = Variable.get("nocodb_token")
    state_key = "affiliate_downloaded_once"

    downloaded_once = Variable.get(state_key, default_var="0") == "1"

    async def fetch_all_data():
        connector = InvolveAsyncConnector(secret_key=secret_key)
        await connector.authenticate()
        all_data = []
        page = 1
        limit = 100
        if not downloaded_once:
            while True:
                data, has_next = await connector.get_conversion(page=str(page), limit=str(limit))
                all_data.extend(data)
                if not has_next:
                    break
                page += 1
            Variable.set(state_key, "1")
        else:
            yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
            while True:
                data, has_next = await connector.get_conversion(start_date=yesterday, end_date=yesterday, page=str(page), limit=str(limit))
                all_data.extend(data)
                if not has_next:
                    break
                page += 1
        return all_data

    all_data = asyncio.run(fetch_all_data())
    connector = NocodbConnector(api_url=nocodb_api_url, token=nocodb_token)
    connector.push(all_data)

with DAG(
    dag_id="affiliate_download_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["affiliate"],
) as dag:
    # download_task = PythonOperator(
    #     task_id="download_affiliate_data",
    #     python_callable=download_and_export_csv_affiliate_data,
    #     provide_context=True,
    # )
    export_to_nocodb_task = PythonOperator(
        task_id="push_affiliate_to_nocodb",
        python_callable=download_and_export_nocodb_affiliate_data,
        provide_context=True,
    ) 
