import httpx
import math
import asyncio
import csv
import os
import time
from datetime import datetime, date
from storage_driver import PostgresDriver, ClickHouseDriver


class StorageExporter:
    def __init__(self, **config):
        pass

    def export(self, data, **settings):
        pass


class NocodbExporter(StorageExporter):
    def __init__(self, **config):
        super().__init__()
        self._api_url = config.get('api_url')
        self._token = config.get('token')

    def export(self, data, **settings):
        batch_size = settings.get('batch_size', 1000)
        headers = {
            "accept": "application/json",
            "xc-token": self._token,
            "Content-Type": "application/json"
        }
        responses = []
        total = len(data)
        for i in range(0, total, batch_size):
            batch = data[i:i+batch_size]
            retry = 0
            while retry < 5:
                try:
                    resp = httpx.post(self._api_url, headers=headers, json=batch, timeout=30.0)
                    resp.raise_for_status()
                    responses.append(resp.json())
                    break  # Thành công thì thoát vòng lặp retry
                except Exception as e:
                    retry += 1
                    if retry < 5:
                        print(f"Retry Nocodb export, attempt {retry+1}/5...")
                        time.sleep(10)
                    else:
                        print(f"Nocodb export failed after 5 attempts: {e}")
                        raise
        return responses


class CsvExporter:
    def __init__(self, **config):
        self._filepath = config.get('filepath')

    def export(self, data, **settings):
        os.makedirs(os.path.dirname(self._filepath), exist_ok=True)
        if not data or not isinstance(data, list):
            raise ValueError("Data must be a list of dicts")
        if len(data) == 0:
            return
        with open(self._filepath, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)


class PostgresSQLExporter(StorageExporter):
    """High-level exporter that uses PostgresDriver for data export operations"""
    
    def __init__(self, **config):
        super().__init__()
        self._table_name = config.get('table_name')
        self._driver = PostgresDriver(**config)
        self._retry_count = config.get('retry_count', 5)
        self._retry_delay = config.get('retry_delay', 10)
    
    def export(self, data, **settings):
        if not data or not isinstance(data, list):
            raise ValueError("Data must be a list of dicts")
        if len(data) == 0:
            return
                
        column_mapping = settings.get('column_mapping', {})
        
        keys = settings.get('keys')
        keys_mode = settings.get('keys_mode', 'include')
        conflict_keys = settings.get('conflict_keys')  # Keys để xác định trùng dữ liệu
        operation_type = settings.get('operation_type', 'insert')  # 'insert', 'upsert', 'merge'

        # Column mapping is applied in PostgresDriver to avoid mutating input data
        
        # Execute operation with retry logic
        for attempt in range(self._retry_count):
            try:
                if operation_type == 'merge' and conflict_keys:
                    # Use MERGE syntax for upsert
                    total_processed = self._driver.merge(
                        data, self._table_name,  keys=keys, keys_mode=keys_mode, merge_keys=conflict_keys, column_mapping=column_mapping
                    )
                    operation = "merge"
                elif operation_type == 'upsert' and conflict_keys:
                    # Use batch_insert with ON CONFLICT for upsert
                    total_processed = self._driver.batch_insert(
                        data, self._table_name, keys=keys, keys_mode=keys_mode, primary_keys=conflict_keys, column_mapping=column_mapping
                    )
                    operation = "upsert"
                else:
                    # Use batch_insert for plain insert
                    total_processed = self._driver.batch_insert(
                        data, self._table_name,  keys=keys, keys_mode=keys_mode, column_mapping=column_mapping
                    )
                    operation = "insert"

                # print(f"Successfully exported {total_processed} records to PostgreSQL table '{self._table_name}' using {operation}")
                # if column_mapping:
                #     print(f"Column mapping applied: {column_mapping}")
                
                return {
                    "exported_records": total_processed, 
                    "table": self._table_name, 
                    "column_mapping": column_mapping,
                    "operation": operation
                }
                
            except Exception as e:
                if attempt < self._retry_count - 1:
                    print(f"Retry PostgreSQL export, attempt {attempt+2}/{self._retry_count}...")
                    time.sleep(self._retry_delay)
                else:
                    print(f"PostgreSQL export failed after {self._retry_count} attempts: {e}")
                    raise
        
        return {"exported_records": 0, "table": self._table_name}
    
    def close(self):
        """Close the underlying driver connection pool"""
        self._driver.close_pool()


class ClickHouseExporter(StorageExporter):
    """High-level exporter that uses ClickHouseDriver for data export operations"""
    
    def __init__(self, **config):
        super().__init__()
        self._table_name = config.get('table_name')
        self._driver = ClickHouseDriver(**config)
        self._retry_count = config.get('retry_count', 3)
        self._retry_delay = config.get('retry_delay', 10)
    
    def export(self, data, **settings):
        """
        Export data to ClickHouse
        
        Args:
            data: List of dictionaries containing data to export
            settings: Additional settings including:
                - batch_size: Number of records to insert per batch (default: 1000)
                - column_mapping: Dictionary mapping source columns to target columns
                - keys: List of keys to include/exclude
                - keys_mode: 'include' or 'exclude' (default: 'include')
        
        Returns:
            dict: Result containing exported_records, table name
        """
        if not data or not isinstance(data, list):
            raise ValueError("Data must be a list of dicts")
        if len(data) == 0:
            return {"exported_records": 0, "table": self._table_name}
        
        column_mapping = settings.get('column_mapping', {})
        keys = settings.get('keys')
        keys_mode = settings.get('keys_mode', 'include')
        batch_size = settings.get('batch_size', 1000)
        
        # Execute operation with retry logic
        total_processed = 0
        for attempt in range(self._retry_count):
            try:
                # ClickHouse doesn't support upsert natively, so we only do insert
                # Split data into batches if batch_size is specified
                if batch_size and batch_size > 0:
                    for i in range(0, len(data), batch_size):
                        batch = data[i:i + batch_size]
                        batch_processed = self._driver.batch_insert(
                            batch, self._table_name, keys=keys, keys_mode=keys_mode, column_mapping=column_mapping
                        )
                        total_processed += batch_processed
                else:
                    # Insert all data at once if batch_size is 0 or None
                    total_processed = self._driver.batch_insert(
                        data, self._table_name, keys=keys, keys_mode=keys_mode, column_mapping=column_mapping
                    )
                
                return {
                    "exported_records": total_processed,
                    "table": self._table_name,
                    "column_mapping": column_mapping,
                    "operation": "insert"
                }
                
            except Exception as e:
                if attempt < self._retry_count - 1:
                    print(f"Retry ClickHouse export, attempt {attempt+2}/{self._retry_count}...")
                    time.sleep(self._retry_delay)
                    total_processed = 0  # Reset on retry
                else:
                    print(f"ClickHouse export failed after {self._retry_count} attempts: {e}")
                    raise
        
        return {"exported_records": total_processed, "table": self._table_name}
    
    def close(self):
        """Close the underlying driver connection"""
        self._driver.close_pool()

    def delete_by_symbol_and_date(self, dates, symbols=None, date_column='date', symbol_column='symbol'):
        """Delete existing rows for specified dates and symbols before inserting new data"""
        if not dates:
            return 0
        if not isinstance(dates, (list, tuple, set)):
            dates = [dates]

        cleaned_dates = []
        # print(dates)
        for value in dates:
            if value is None:
                continue
            if isinstance(value, datetime):
                value = value.date()
            if isinstance(value, str):
                try:
                    value = datetime.strptime(value, "%Y-%m-%d").date()
                except ValueError:
                    continue
            if isinstance(value, date):
                cleaned_dates.append(value)

        if not cleaned_dates:
            return 0

        # Prepare symbols list if provided
        cleaned_symbols = []
        if symbols:
            if not isinstance(symbols, (list, tuple, set)):
                symbols = [symbols]
            cleaned_symbols = [str(s).replace("'", "''") for s in symbols if s is not None]  # Escape single quotes

        # Build WHERE clause
        where_conditions = [f"`{date_column}` = %(target_date)s"]
        if cleaned_symbols:
            # Use tuple format for IN clause (safe since symbols are validated)
            symbols_tuple = "('" + "','".join(cleaned_symbols) + "')"
            where_conditions.append(f"`{symbol_column}` IN {symbols_tuple}")

        query = f"""
            ALTER TABLE `{self._table_name}`
            DELETE WHERE {' AND '.join(where_conditions)}
            SETTINGS mutations_sync = 1
        """
        
        deleted_count = 0
        for value in cleaned_dates:
            params = {'target_date': value}
            self._driver.execute_query(query, params)
            deleted_count += 1
        return deleted_count
