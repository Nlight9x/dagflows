from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import asyncio
import os
import json

from utils.affiliate_connector import InvolveAsyncConnector
from utils.storage_exporter import CsvExporter, NocodbExporter
from utils.ads_network_connector import GalaksionAsyncConnector


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
            # Các lần sau: Chỉ lấy ngày hôm qua
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
    secret_key = Variable.get("involve_asia_secret_key_1")
    nocodb_api_url = Variable.get("nocodb_involve_asia_conversion_put_endpoint")
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
    connector = NocodbExporter(api_url=nocodb_api_url, token=nocodb_token)
    connector.export(all_data)


def download_and_export_nocodb_galaksion_data(**context):
    galaksion_credential = Variable.get("galaksion_credential")
    cred = json.loads(galaksion_credential)
    galaksion_email = cred["email"]
    galaksion_password = cred["password"]
    
    nocodb_api_url = Variable.get("nocodb_galaksion_statistics_put_endpoint")
    nocodb_token = Variable.get("nocodb_token")
    state_key = "galaksion_downloaded_once"
    downloaded_once = Variable.get(state_key, default_var="0") == "1"

    async def load_and_push_for_day(day, connector, exporter, limit, buffer_size, order_by):
        offset = 0
        buffer = []
        def has_money_gt_zero(records):
            if not records:
                return False
            return float(records[0].get("money", 0)) > 0
        def push_buffer():
            if buffer:
                exporter.export(buffer.copy())
                buffer.clear()
        while True:
            data, has_next = await connector.get_reports(date_from=day, date_to=day, limit=limit, offset=offset, order_by=order_by)
            if data:
                buffer.extend(data)
                if len(buffer) >= buffer_size:
                    push_buffer()
            if not (has_next and has_money_gt_zero(data)):
                break
            offset += limit
        push_buffer()

    async def fetch_and_push_galaksion_data():
        connector = GalaksionAsyncConnector(email=galaksion_email, password=galaksion_password)
        await connector.authenticate()
        limit = 100
        buffer_size = 1000
        order_by = {"field": "money", "direction": "desc"}
        exporter = NocodbExporter(api_url=nocodb_api_url, token=nocodb_token)
        def format_day(dt):
            return dt.strftime("%Y-%m-%d 00:00:00")
        if not downloaded_once:
            for i in range(14, 0, -1):
                day_dt = datetime.now() - timedelta(days=i)
                day = format_day(day_dt)
                await load_and_push_for_day(day, connector, exporter, limit, buffer_size, order_by)
            Variable.set(state_key, "1")
        else:
            yesterday_dt = datetime.now() - timedelta(days=1)
            yesterday = format_day(yesterday_dt)
            await load_and_push_for_day(yesterday, connector, exporter, limit, buffer_size, order_by)

    asyncio.run(fetch_and_push_galaksion_data())


with DAG(
    dag_display_name="[ADS] - Download report data",
    dag_id="ads_download_report_data",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["affiliate"],
) as dag:
    export_to_nocodb_task = PythonOperator(
        task_display_name="Download InvolveAsia conversions",
        task_id="get_and_save_involve_data_to_nocodb",
        python_callable=download_and_export_nocodb_affiliate_data,
        provide_context=True,
    )

    export_to_nocodb_galaksion_task = PythonOperator(
        task_display_name="Download Galaksion statistics",
        task_id="get_and_save_galaksion_data_to_nocodb",
        python_callable=download_and_export_nocodb_galaksion_data,
        provide_context=True,
    )