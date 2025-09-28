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
    
    def batch_insert(self, data, table_name, column_mapping=None, columns=None, merge_key_columns=None, batch_size=1000):
       
        if not data and len(data) <= 0:
            return 0
        
        # Get columns from data if not provided
        if not columns:
            columns = list(data[0].keys())
        
        # Resolve target columns using mapping (without mutating data) 
        column_mapping = column_mapping or {}
        target_columns = [column_mapping.get(fd, fd) for fd in columns]

        columns_str = ', '.join(target_columns)
        placeholders = ', '.join(['%s'] * len(columns))
        
        # Build query
        if merge_key_columns and len(merge_key_columns) > 0:
            # UPSERT path
            update_columns = [col for col in target_columns if col not in merge_key_columns]
            update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])
            insert_query = f"""
                INSERT INTO {table_name} ({columns_str}) 
                VALUES ({placeholders})
                ON CONFLICT ({', '.join(merge_key_columns)}) 
                DO UPDATE SET {update_clause}
            """
        else:
            # Plain INSERT path
            insert_query = f"""
                INSERT INTO {table_name} ({columns_str}) 
                VALUES ({placeholders})
            """
        
        # Prepare data
        values_list = []
        for record in data:
            values = [record.get(col) for col in columns]
            values_list.append(values)
        
        # Insert in batches
        total_inserted = 0
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                for i in range(0, len(values_list), batch_size):
                    batch = values_list[i:i+batch_size]
                    execute_values( cursor, insert_query, batch, template=None, page_size=batch_size )
                    total_inserted += len(batch)
                conn.commit()
        return total_inserted
    
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
        
        batch_size = settings.get('batch_size', 1000)
        
        column_mapping = settings.get('column_mapping', {})
        merge_key_columns = settings.get('merge_key_columns')
        columns = settings.get('columns')

        # Column mapping is applied in PostgresDriver to avoid mutating input data
        
        # Execute operation with retry logic
        for attempt in range(self._retry_count):
            try:
                total_processed = self._driver.batch_insert(
                    data, self._table_name, column_mapping=column_mapping,
                    columns=columns, merge_key_columns=merge_key_columns, batch_size=batch_size)

                print(f"Successfully exported {total_processed} records to PostgreSQL table '{self._table_name}'")
                if column_mapping:
                    print(f"Column mapping applied: {column_mapping}")
                
                return {
                    "exported_records": total_processed, 
                    "table": self._table_name, 
                    "column_mapping": column_mapping,
                    "operation": "upsert" if merge_key_columns and len(merge_key_columns) > 0 else "insert"
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
