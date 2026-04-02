import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import pool
from contextlib import contextmanager


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

    def _resolve_source_spec(self, source_spec, record):
        if isinstance(source_spec, str):
            return record.get(source_spec)
        if isinstance(source_spec, list):
            for source_key in source_spec:
                if isinstance(source_key, str):
                    value = record.get(source_key)
                    if value is not None:
                        return value
            return None
        if isinstance(source_spec, dict):
            source_key = source_spec.get("source")
            default_value = source_spec.get("default")
            if isinstance(source_key, str):
                return record.get(source_key, default_value)
            return default_value
        return source_spec

    def _prepare_projection(self, data, keys: list = None, column_mapping: dict = None):
        """
        Returns:
            source_fields: list of source keys/specs used to read record values
            target_columns: list of DB columns used in SQL
            values_builder: callable(record) -> list of values aligned with target_columns
        """
        if not data or len(data) == 0:
            return [], [], lambda _r: []

        # Only one mapping format is supported: target_column -> source_spec
        # source_spec can be: str | list[str] | {"source": str, "default": any} | literal
        if not column_mapping:
            available_targets = list(data[0].keys())
            mapping = {k: k for k in available_targets}
        elif isinstance(column_mapping, dict):
            available_targets = list(column_mapping.keys())
            mapping = column_mapping
        else:
            raise ValueError("column_mapping must be a dict of target_column -> source_spec")

        if keys is None:
            target_columns = available_targets
        else:
            target_columns = [k for k in keys if k in available_targets]

        source_fields = [mapping.get(col, col) for col in target_columns]

        def values_builder(record):
            return [self._resolve_source_spec(spec, record) for spec in source_fields]

        return source_fields, target_columns, values_builder
    
    def batch_insert(self, data, table_name, keys=None, primary_keys=None, column_mapping=None):
       
        if not data or len(data) <= 0:
            return 0

        source_fields, target_cols, values_builder = self._prepare_projection(data=data, keys=keys, column_mapping=column_mapping)
        if not target_cols:
            raise ValueError("No columns to insert after applying keys and mapping")

        target_cols_str = '"' + '", "'.join(target_cols) + '"'
        
        # Build query
        if primary_keys and len(primary_keys) > 0:
            # conflict keys are expected to be target DB columns
            primary_cols = primary_keys
            # UPSERT path
            update_cols = [col for col in target_cols if col not in primary_cols]
            primary_cols_str = '"' + '", "'.join(primary_cols) + '"'
            if update_cols:
                update_clause = ', '.join([f"\"{col}\" = EXCLUDED.\"{col}\"" for col in update_cols])
                conflict_action_sql = f"DO UPDATE SET {update_clause}"
            else:
                conflict_action_sql = "DO NOTHING"
            insert_query = f"""
                INSERT INTO "{table_name}" ({target_cols_str}) 
                VALUES %s
                ON CONFLICT ({primary_cols_str}) 
                {conflict_action_sql}
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
            values = values_builder(record)
            batch.append(values)
        
        # Insert in batches
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                print(insert_query)
                execute_values(cursor, insert_query, batch)
                conn.commit()
        return len(batch)

    def merge(self, data, table_name, keys=None, merge_keys=None, column_mapping=None):
        """Merge data using PostgreSQL MERGE syntax (PostgreSQL 15+)"""
        if not data:
            return 0
        if not merge_keys or len(merge_keys) == 0:
            raise ValueError("merge_keys must be provided for MERGE operation")

        source_columns, target_columns, values_builder = self._prepare_projection(data=data, keys=keys, column_mapping=column_mapping)
        if not target_columns:
            raise ValueError("No columns to merge after applying keys filter")

        # merge_keys are expected to be target DB columns
        mapped_merge_keys = merge_keys
        # Validate all merge keys are present in target columns
        missing_merge_cols = [k for k in mapped_merge_keys if k not in target_columns]
        if missing_merge_cols:
            raise ValueError(f"merge_keys not present in target columns: {missing_merge_cols}")
        
        # Prepare data
        values_list = []
        for record in data:
            values = values_builder(record)
            values_list.append(values)
        
        # Build MERGE query
        columns_str = ', '.join([f'"{c}"' for c in target_columns])
        
        # Build WHEN MATCHED clause (update non-key columns)
        update_columns = [col for col in target_columns if col not in mapped_merge_keys]
        if update_columns:
            update_clause = ', '.join([f'"{col}" = source."{col}"' for col in update_columns])
            when_matched_sql = f'WHEN MATCHED THEN\n                UPDATE SET {update_clause}'
        else:
            when_matched_sql = 'WHEN MATCHED THEN\n                DO NOTHING'
        
        # Build WHEN NOT MATCHED clause (insert all columns)
        insert_columns = ', '.join([f'"{c}"' for c in target_columns])
        insert_values = ', '.join([f'source."{col}"' for col in target_columns])
        
        merge_query = f"""
            MERGE INTO "{table_name}" AS target
            USING (VALUES %s) AS source ({columns_str})
            ON ({', '.join([f'target."{col}" = source."{col}"' for col in mapped_merge_keys])})
            {when_matched_sql}
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


class ClickHouseDriver:
    """Low-level ClickHouse driver with connection management and basic operations"""
    
    def __init__(self, **config):
        self._host = config.get('host', 'localhost')
        self._port = config.get('port', 9000)
        self._database = config.get('database', 'default')
        self._user = config.get('user', 'default')
        self._password = config.get('password', '')
        self._client = None
    
    def _get_client(self):
        """Get or create ClickHouse client"""
        if self._client is None:
            try:
                from clickhouse_driver import Client
            except ImportError:
                raise ImportError("clickhouse-driver not installed. Please install it: pip install clickhouse-driver")
            
            self._client = Client(
                host=self._host,
                port=self._port,
                database=self._database,
                user=self._user,
                password=self._password
            )
        return self._client
    
    def execute_query(self, query, params=None, fetch=False):
        """Execute a query and optionally fetch results"""
        client = self._get_client()
        if params:
            result = client.execute(query, params)
        else:
            result = client.execute(query)
        
        if fetch:
            return result
        return len(result) if isinstance(result, list) else 0

    def _resolve_source_spec(self, source_spec, record):
        if isinstance(source_spec, str):
            return record.get(source_spec)
        if isinstance(source_spec, list):
            for source_key in source_spec:
                if isinstance(source_key, str):
                    value = record.get(source_key)
                    if value is not None:
                        return value
            return None
        if isinstance(source_spec, dict):
            source_key = source_spec.get("source")
            default_value = source_spec.get("default")
            if isinstance(source_key, str):
                return record.get(source_key, default_value)
            return default_value
        return source_spec
    
    def batch_insert(self, data, table_name, keys=None, column_mapping=None):
        """
        Batch insert data into ClickHouse table
        
        Args:
            data: List of dictionaries containing data to insert
            table_name: Target table name
            keys: List of target columns to export (optional)
            column_mapping: Dictionary mapping target columns to source specs (optional)
        
        Returns:
            int: Number of records inserted
        """
        if not data or len(data) == 0:
            return 0
        
        # Only one mapping format is supported: target_column -> source_spec
        if not column_mapping:
            available_targets = list(data[0].keys())
            mapping = {k: k for k in available_targets}
        elif isinstance(column_mapping, dict):
            available_targets = list(column_mapping.keys())
            mapping = column_mapping
        else:
            raise ValueError("column_mapping must be a dict of target_column -> source_spec")

        if keys is None:
            target_cols = available_targets
        else:
            target_cols = [k for k in keys if k in available_targets]
        if not target_cols:
            raise ValueError("No columns to insert after applying keys and mapping")

        source_specs = [mapping.get(col, col) for col in target_cols]
        
        # Prepare data rows
        rows = []
        for record in data:
            values = [self._resolve_source_spec(spec, record) for spec in source_specs]
            rows.append(tuple(values))
        
        # Insert data into ClickHouse
        column_names_str = ', '.join([f'`{col}`' for col in target_cols])
        insert_query = f"INSERT INTO `{self._database}`.`{table_name}` ({column_names_str}) VALUES"
        
        client = self._get_client()
        client.execute(insert_query, rows)
        
        return len(rows)
    
    def close_pool(self):
        """Close ClickHouse client connection"""
        if self._client:
            self._client.disconnect()
            self._client = None
            print("ClickHouse connection closed")
