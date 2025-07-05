from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import asyncio
import os
import json
import httpx
import pendulum

from utils.affiliate_connector import InvolveAsyncConnector
from utils.storage_exporter import CsvExporter, NocodbExporter
from utils.ads_network_connector import GalaksionAsyncConnector

# Chỉ dùng cho dữ liệu Galaksion
from datetime import datetime as dt

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
            # Các lần sau: lấy ngày theo execution_date
            execution_date = context['execution_date']
            day = execution_date.strftime("%Y-%m-%d")
            while True:
                data, has_next = await connector.get_conversion(start_date=day, end_date=day, page=str(page), limit=str(limit))
                all_data.extend(data)
                if not has_next:
                    break
                page += 1
        return all_data

    def export_csv(data):
        CsvExporter(filepath=output_path).export(data)

    all_data = asyncio.run(fetch_all_data())
    export_csv(all_data)


def download_and_export_nocodb_involve_data(**context):
    secret_key = Variable.get("involve_asia_secret_key_1")
    nocodb_api_url = Variable.get("nocodb_involve_asia_conversion_put_endpoint")
    nocodb_token = Variable.get("nocodb_token")
    state_key = "involve_asia_downloaded_once"

    downloaded_once = Variable.get(state_key, default_var="0") == "1"

    async def fetch_data_for_range(start_date, end_date, connector):
        all_data = []
        page = 1
        limit = 100
        df_filters = {'preferred_currency': 'USD'}
        while True:
            data, has_next = await connector.get_conversion(start_date=start_date, end_date=end_date, page=str(page), limit=str(limit), **df_filters)
            all_data.extend(data)
            if not has_next:
                break
            page += 1
        return all_data

    async def fetch_all_data():
        connector = InvolveAsyncConnector(secret_key=secret_key)
        await connector.authenticate()
        all_data = []
        
        execution_date = context.get('execution_date') if 'execution_date' in context else None
        
        if not downloaded_once:
            # Lần đầu: lấy toàn bộ dữ liệu lịch sử
            page = 1
            limit = 100
            df_filters = {'preferred_currency': 'USD'}
            while True:
                data, has_next = await connector.get_conversion(page=str(page), limit=str(limit), **df_filters)
                all_data.extend(data)
                if not has_next:
                    break
                page += 1
            Variable.set(state_key, "1")
        else:
            # Các lần sau: lấy ngày theo execution_date
            day = execution_date if execution_date else datetime.now()
            day_str = day.strftime("%Y-%m-%d")
            all_data = await fetch_data_for_range(day_str, day_str, connector)
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

    def transform_data(data):
        transformed_data = []
        for item in data:
            new_item = {k: v for k, v in item.items() if k != 'id'}
            # Tách campaign_id từ campaign
            if 'campaign' in new_item and isinstance(new_item['campaign'], str):
                parts = new_item['campaign'].strip().split(' ', 1)
                if parts and parts[0].isdigit():
                    new_item['campaign_id'] = parts[0]
            if 'day' in new_item:
                try:
                    day_obj = dt.strptime(new_item['day'], '%d/%m/%Y')
                    new_item['date'] = day_obj.strftime('%Y-%m-%d')
                    del new_item['day']
                except Exception:
                    pass
            transformed_data.append(new_item)
        return transformed_data

    async def load_and_push_for_day(date_from, date_to, connector, exporter, limit, buffer_size, order_by, group_by):
        offset = 0
        buffer = []
        accumulated_money = 0.0
        total_money = None
        total_data = None  # Lưu toàn bộ thông tin total
        percent_threshold = float(Variable.get('galaksion_money_percent_threshold', default_var='0.7'))

        def has_money_gt_zero(records):
            if not records:
                return False
            return float(records[0].get("money", 0)) > 0
        
        def push_buffer():
            if buffer:
                exporter.export(transform_data(buffer.copy()))
                buffer.clear()

        async def fetch_with_retry(retry_count=3, delay_seconds=5):
            nonlocal offset
            for attempt in range(retry_count):
                try:
                    response, has_next = await connector.get_reports(
                        date_from=date_from, date_to=date_to, limit=limit, offset=offset, order_by=order_by, group_by=group_by
                    )
                    await asyncio.sleep(delay_seconds)
                    return response, has_next
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 429 and attempt < retry_count - 1:
                        wait_time = (attempt + 1) * delay_seconds * 2
                        print(f"429 Too Many Requests, waiting {wait_time}s before retry...")
                        await asyncio.sleep(wait_time)
                        continue
                    raise
                except Exception as e:
                    raise
            return None, False

        while True:
            response, has_next = await fetch_with_retry()
            if not response:
                break
            rows = response.get('rows', [])
            buffer.extend(rows)
            # Cộng dồn money của tất cả các row đã load
            accumulated_money += sum(float(item.get('money', 0)) for item in rows)
            # Lấy total_money và total_data từ response đầu tiên
            if total_money is None and 'total' in response:
                try:
                    total_money = float(response['total']['money'])
                    # Lưu lại toàn bộ thông tin total
                    total_data = {
                        'date': date_from[:10], 'campaign': 'Total', 'campaign_id': 'Total', 'zone': 'Total', 'geo': 'Total',
                        **response['total']
                    }
                except Exception:
                    total_money = None
            if len(buffer) >= buffer_size:
                push_buffer()
            if (total_money and accumulated_money >= percent_threshold * total_money) or not has_next or not has_money_gt_zero(rows):
                print(f"Stopped: Reached {percent_threshold*100:.2f}% of total money or finished paging. Accumulated: {accumulated_money}/{total_money}")
                break
            offset += limit
        # Đẩy nốt buffer cuối cùng nếu còn
        push_buffer()
        # Đẩy thông tin total vào NocoDB
        if total_data:
            exporter.export([total_data])

    async def fetch_and_push_galaksion_data():
        connector = GalaksionAsyncConnector(email=galaksion_email, password=galaksion_password)
        await connector.authenticate()
        limit = 100
        buffer_size = 500
        order_by = {"field": "money", "direction": "desc"}
        group_by = ["day", "campaign", "zone", "geo"]
        exporter = NocodbExporter(api_url=nocodb_api_url, token=nocodb_token)

        def get_date_range(dt):
            day_str = dt.strftime("%Y-%m-%d")
            return f"{day_str} 00:00:00", f"{day_str} 23:59:59"
        execution_date = context.get('execution_date') if 'execution_date' in context else None
        if not downloaded_once:
            start_day = execution_date if execution_date else datetime.now()
            days = [start_day - timedelta(days=i) for i in range(14)]
        else:
            day = execution_date if execution_date else datetime.now()
            days = [day]
        
        for dt in days:
            date_from, date_to = get_date_range(dt)
            await load_and_push_for_day(date_from, date_to, connector, exporter, limit, buffer_size, order_by, group_by)
        Variable.set(state_key, "1")

    asyncio.run(fetch_and_push_galaksion_data())


local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")
with DAG(
    dag_display_name="[ADS] - Download report data",
    dag_id="ads_download_report_data",
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    schedule_interval="0 10 * * *",
    catchup=False,
    tags=["affiliate"],
) as dag:
    export_to_nocodb_task = PythonOperator(
        task_display_name="Download InvolveAsia conversions",
        task_id="get_and_save_involve_data_to_nocodb",
        python_callable=download_and_export_nocodb_involve_data,
        provide_context=True,
        retries=3,
        retry_delay=timedelta(minutes=2),
    )

    export_to_nocodb_galaksion_task = PythonOperator(
        task_display_name="Download Galaksion statistics",
        task_id="get_and_save_galaksion_data_to_nocodb",
        python_callable=download_and_export_nocodb_galaksion_data,
        provide_context=True,
    )