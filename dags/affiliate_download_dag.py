from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Variable
from datetime import datetime, timedelta
import asyncio
import os
import json
import httpx
import pendulum

from utils.affiliate_connector import InvolveAsyncConnector, TripComAsyncConnector
from utils.storage_exporter import CsvExporter, NocodbExporter, PostgresSQLExporter
from utils.ads_network_connector import GalaksionAsyncConnector

# Chỉ dùng cho dữ liệu Galaksion
from datetime import datetime as dt


def _get_data_date(**context):
    logical_date = context.get('logical_date') if 'logical_date' in context else datetime.now()
    return (logical_date - timedelta(days=1)).date()


def download_and_export_csv_affiliate_data(**context):
    secret_key = Variable.get("affiliate_secret_key")
    output_path = os.path.join(os.path.dirname(__file__), "../data/affiliate_data.csv")
    state_key = "affiliate_downloaded_once"

    # Kiểm tra trạng thái đã từng chạy chưa
    downloaded_once = Variable.get(state_key, default="0") == "1"

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
            execution_date = _get_data_date(**context)
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

    downloaded_once = Variable.get(state_key, default="0") == "1"

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
        
        execution_date = _get_data_date(**context)
        print(f"Data date: {execution_date}")
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
            day_str = execution_date.strftime("%Y-%m-%d")
            all_data = await fetch_data_for_range(day_str, day_str, connector)
        return all_data

    all_data = asyncio.run(fetch_all_data())
    connector = NocodbExporter(api_url=nocodb_api_url, token=nocodb_token)
    connector.export(all_data)


def download_and_export_nocodb_galaksion_data(**context):
    galaksion_token = Variable.get("galaksion_token")
    
    nocodb_api_url = Variable.get("nocodb_galaksion_statistics_put_endpoint")
    nocodb_token = Variable.get("nocodb_token")
    state_key = "galaksion_downloaded_once"
    downloaded_once = Variable.get(state_key, default="0") == "1"

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

    async def get_hoath_campaign_ids(connector, date_from, date_to):
        """Bước 1: Lấy campaign_id của những campaign có 'hoath' trong tên"""
        campaign_ids = []
        offset = 0
        limit = 100
        order_by = {"field": "money", "direction": "desc"}
        group_by = ["campaign"]  # Chỉ group by campaign
        
        while True:
            try:
                response, has_next = await connector.get_reports(
                    date_from=date_from, date_to=date_to, limit=limit, offset=offset, order_by=order_by, group_by=group_by
                )
                
                rows = response.get('rows', [])
                for row in rows:
                    campaign_name = row.get('campaign', '')
                    if 'hoath' in campaign_name.lower():
                        # Tách campaign_id từ campaign name (format: "123 Campaign Name")
                        parts = campaign_name.strip().split(' ', 1)
                        if parts and parts[0].isdigit():
                            campaign_ids.append(int(parts[0]))
                
                if not has_next:
                    break
                offset += limit
                
            except Exception as e:
                print(f"Error getting hoath campaigns: {e}")
                raise

        print(f"Found {len(campaign_ids)} campaigns with 'hoath': {campaign_ids}")
        return campaign_ids

    async def load_and_push_for_day(date_from, date_to, connector, exporter, limit, buffer_size, order_by, group_by, campaign_ids=None):
        offset = 0
        buffer = []
        accumulated_money = 0.0
        total_money = None
        total_data = None  # Lưu toàn bộ thông tin total
        percent_threshold = float(Variable.get('galaksion_money_percent_threshold', default='0.99'))

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
                    # Bước 2: Thêm filter campaigns nếu có
                    params = {
                        'date_from': date_from, 
                        'date_to': date_to, 
                        'limit': limit, 
                        'offset': offset, 
                        'order_by': order_by, 
                        'group_by': group_by
                    }
                    if campaign_ids:
                        params['campaigns'] = campaign_ids
                    
                    response, has_next = await connector.get_reports(**params)
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
        connector = GalaksionAsyncConnector(token=galaksion_token)
        limit = 100
        buffer_size = 500
        order_by = {"field": "money", "direction": "desc"}
        group_by = ["day", "campaign", "zone", "geo"]
        exporter = NocodbExporter(api_url=nocodb_api_url, token=nocodb_token)

        def get_date_range(dt):
            day_str = dt.strftime("%Y-%m-%d")
            return f"{day_str} 00:00:00", f"{day_str} 23:59:59"
        
        execution_date = _get_data_date(**context)
        print(f"Data date: {execution_date}")
        if not downloaded_once:
            days = [execution_date - timedelta(days=i) for i in range(14)]
        else:
            days = [execution_date]
        
        for dt in days:
            date_from, date_to = get_date_range(dt)
            # Bước 1: Lấy campaign_ids có 'hoath' cho đúng ngày này
            campaign_ids = await get_hoath_campaign_ids(connector, date_from, date_to)
            # Bước 2: Lấy dữ liệu với campaign_ids vừa tìm được
            if len(campaign_ids) > 0:
                await load_and_push_for_day(date_from, date_to, connector, exporter, limit, buffer_size, order_by, group_by, campaign_ids)
        Variable.set(state_key, "1")

    asyncio.run(fetch_and_push_galaksion_data())


def download_and_export_postgres_tripcom_data(**context):
    """Download Trip.com conversion data and export to PostgreSQL"""
    # Get Trip.com cookies from Airflow Variable
    tripcom_cookies = Variable.get("tripcom_cookies")
    
    # Get PostgreSQL connection info from Airflow Variable
    postgres_config = Variable.get("postgres_db_connection_info")
    postgres_config = json.loads(postgres_config)
    
    # Get table name from Variable
    table_name = Variable.get("tripcom_table_name", default="Tripcom Conversions")
    
    keys = Variable.get("tripcom_keys", default=None)
    keys_mode = Variable.get("tripcom_keys_mode", default="include")
    conflict_keys = Variable.get("tripcom_conflict_keys", default='["orderId"]')
    operation_type = Variable.get("tripcom_operation_type", default="upsert")
    column_mapping = Variable.get("tripcom_column_mapping", default=None)

    async def fetch_and_export_tripcom_data():
        """Fetch Trip.com data page by page and export to PostgreSQL immediately"""
        connector = TripComAsyncConnector(cookies=tripcom_cookies)
        await connector.authenticate()
        
        # Calculate date range: 3 months from current date
        end_date = context.get('logical_date') if 'logical_date' in context else datetime.now()
        start_date = end_date - timedelta(days=90)  # Approximately 3 months
        
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")
        
        print(f"Fetching Trip.com data from {start_date_str} to {end_date_str}")
        
        # Initialize PostgreSQL exporter
        exporter = PostgresSQLExporter(table_name=table_name, **postgres_config)
        
        page = 1
        page_size = 100
        total_exported = 0
        
        try:
            while True:
                data, has_next = await connector.get_conversion(
                    start_date=start_date_str, 
                    end_date=end_date_str,
                    page=page,
                    page_size=page_size
                )
                
                if data:
                    # print(f"Page {page}: Fetched {len(data)} records from Trip.com")
                    # Export this page immediately to PostgreSQL
                    result = exporter.export(
                        data=data,
                        column_mapping=json.loads(column_mapping) if column_mapping is not None else None,
                        keys=json.loads(keys) if keys is not None else None,
                        keys_mode=keys_mode,
                        conflict_keys=json.loads(conflict_keys) if conflict_keys is not None else None,
                        operation_type=operation_type,
                        retry_count=3,
                        retry_delay=5
                    )
                    
                    total_exported += result['exported_records']
                    # print(f"Page {page}: Exported {result['exported_records']} records to PostgreSQL")
                
                if not has_next:
                    break
                    
                page += 1
                
        except Exception as e:
            print(f"Error fetching Trip.com data: {e}")
            raise
            
        print(f"Total exported: {total_exported} records to PostgreSQL table '{table_name}'")
        return total_exported

    # Execute the workflow
    asyncio.run(fetch_and_export_tripcom_data())


local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")
with DAG(
    dag_display_name="[ADS] - Download report data",
    dag_id="ads_download_report_data",
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    schedule="0 10 * * *",
    catchup=False,
    # tags=["affiliate"],
) as dag:
    export_involve_to_nocodb_task = PythonOperator(
        task_display_name="Download InvolveAsia conversions",
        task_id="get_and_save_involve_data_to_nocodb",
        python_callable=download_and_export_nocodb_involve_data,
        # provide_context=True,
        retries=3,
        retry_delay=timedelta(minutes=2),
    )

    export_galaksion_to_nocodb_task = PythonOperator(
        task_display_name="Download Galaksion statistics",
        task_id="get_and_save_galaksion_data_to_nocodb",
        python_callable=download_and_export_nocodb_galaksion_data,
        # provide_context=True,
    )

    # export_tripcom_to_postgres_task = PythonOperator(
    #     task_display_name="Download Trip.com conversions",
    #     task_id="get_and_save_tripcom_data_to_postgres",
    #     python_callable=download_and_export_postgres_tripcom_data,
    #     # provide_context=True,
    #     retries=3,
    #     retry_delay=timedelta(minutes=2),
    # )
    export_galaksion_to_nocodb_task >> export_involve_to_nocodb_task
    # >> export_tripcom_to_postgres_task