import httpx
import math
import asyncio
import csv
import os
import time
import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import pool
from contextlib import contextmanager


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
        super().__init__()
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


class PostgresDriver:
    """Low-level PostgreSQL driver with connection pooling and basic operations"""
    
    def __init__(self, **config):
        host = config.get('host')
        port = config.get('port', 5432)
        database = config.get('database')
        schema = config.get('schema', 'public')
        user = config.get('user')
        password = config.get('password')
        
        self._connection_string = f"host={host} port={port} dbname={database} user={user} password={password} options=-csearch_path={schema}"
        self._min_conn = config.get('min_conn', 1)
        self._max_conn = config.get('max_conn', 10)

        # Initialize connection pool
        self._pool = None
        self._initialize_pool()
    
    def _initialize_pool(self):
        """Initialize connection pool"""
        try:
            self._pool = psycopg2.pool.ThreadedConnectionPool( self._min_conn, self._max_conn, self._connection_string)
            print(f"PostgreSQL connection pool initialized: {self._min_conn}-{self._max_conn} connections")
        except Exception as e:
            print(f"Failed to initialize connection pool: {e}")
            self._pool = None
    
    @contextmanager
    def get_connection(self):
        """Get connection from pool with automatic cleanup"""
        if not self._pool:
            raise Exception("Connection pool not initialized")
        
        conn = None
        try:
            conn = self._pool.getconn()
            yield conn
        finally:
            if conn:
                self._pool.putconn(conn)
    
    def execute_query(self, query, params=None, fetch=False):
        """Execute a query and optionally fetch results"""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                if fetch:
                    return cursor.fetchall()
                conn.commit()
                return cursor.rowcount
    
    def batch_insert(self, data, table_name, keys=None, keys_mode="include", primary_keys=None, column_mapping=None):
       
        if not data or len(data) <= 0:
            return 0

        if keys is None:
            target_keys = list(data[0].keys())
        else:
            if keys_mode not in ("include", "exclude"):
                raise ValueError("keys_mode must be 'include' or 'exclude'")
            if keys_mode == "include":
                target_keys = [k for k in keys if k in data[0].keys()]
            else:
                target_keys = [k for k in data[0].keys() if k not in keys]
        target_cols = [column_mapping.get(tk, tk) for tk in target_keys] if column_mapping else target_keys

        target_cols_str = '"' + '", "'.join(target_cols) + '"'
        
        # Build query
        if primary_keys and len(primary_keys) > 0:
            primary_cols = [column_mapping.get(pk, pk) for pk in primary_keys]
            # UPSERT path
            update_cols = [col for col in target_cols if col not in primary_cols]
            update_clause = ', '.join([f"\"{col}\" = EXCLUDED.\"{col}\"" for col in update_cols])
            primary_cols_str = '"' + '", "'.join(primary_cols) + '"'
            insert_query = f"""
                INSERT INTO "{table_name}" ({target_cols_str}) 
                VALUES %s
                ON CONFLICT ({primary_cols_str}) 
                DO UPDATE SET {update_clause}
            """
        else:
            # Plain INSERT path
            insert_query = f"""
                INSERT INTO "{table_name}" ({target_cols_str}) 
                VALUES %s
            """
        # Prepare data
        batch = []
        for record in data:
            values = [record.get(k) for k in target_keys]
            batch.append(values)
        
        # Insert in batches
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                execute_values(cursor, insert_query, batch)
                conn.commit()
        return len(batch)

    def merge(self, data, table_name, keys=None, keys_mode="include", merge_keys=None, column_mapping=None):
        """Merge data using PostgreSQL MERGE syntax (PostgreSQL 15+)"""
        if not data:
            return 0
        if not merge_keys or len(merge_keys) == 0:
            raise ValueError("merge_keys must be provided for MERGE operation")
        
        # Determine source columns
        if keys is None:
            source_columns = list(data[0].keys())
        else:
            if keys_mode not in ("include", "exclude"):
                raise ValueError("keys_mode must be 'include' or 'exclude'")
            if keys_mode == "include":
                source_columns = [c for c in keys if c in data[0].keys()]
            else:
                source_columns = [c for c in data[0].keys() if c not in keys]
        
        if not source_columns:
            raise ValueError("No columns to merge after applying keys filter")
        
        # Resolve target columns using mapping (without mutating data) 
        column_mapping = column_mapping or {}
        target_columns = [column_mapping.get(fd, fd) for fd in source_columns]
        # Map merge_keys (from data keys) to target DB columns
        mapped_merge_keys = [column_mapping.get(k, k) for k in merge_keys]
        # Validate all merge keys are present in target columns
        missing_merge_cols = [k for k in mapped_merge_keys if k not in target_columns]
        if missing_merge_cols:
            raise ValueError(f"merge_keys not present in target columns: {missing_merge_cols}")
        
        # Prepare data
        values_list = []
        for record in data:
            values = [record.get(col) for col in source_columns]
            values_list.append(values)
        
        # Build MERGE query
        columns_str = ', '.join([f'"{c}"' for c in target_columns])
        
        # Build WHEN MATCHED clause (update non-key columns)
        update_columns = [col for col in target_columns if col not in mapped_merge_keys]
        update_clause = ', '.join([f'"{col}" = source."{col}"' for col in update_columns])
        
        # Build WHEN NOT MATCHED clause (insert all columns)
        insert_columns = ', '.join([f'"{c}"' for c in target_columns])
        insert_values = ', '.join([f'source."{col}"' for col in target_columns])
        
        merge_query = f"""
            MERGE INTO "{table_name}" AS target
            USING (VALUES %s) AS source ({columns_str})
            ON ({', '.join([f'target."{col}" = source."{col}"' for col in mapped_merge_keys])})
            WHEN MATCHED THEN
                UPDATE SET {update_clause}
            WHEN NOT MATCHED THEN
                INSERT ({insert_columns}) VALUES ({insert_values})
        """
        
        print(f"MERGE Query: {merge_query}")
        
        # Execute merge
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                execute_values(cursor, merge_query, values_list)
                conn.commit()
        
        return len(values_list)
    
    def delete(self, table_name, where_clause, params=None):
        """Delete records based on WHERE clause"""
        delete_query = f"DELETE FROM {table_name} WHERE {where_clause}"
        return self.execute_query(delete_query, params)
    
    def close_pool(self):
        """Close all connections in the pool"""
        if self._pool:
            self._pool.closeall()
            print("PostgreSQL connection pool closed")


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
