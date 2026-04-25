-- ClickHouse table schema for securities minute data
-- Run this SQL to create the table before using the DAG
-- 
-- Usage:
--   clickhouse-client --query "$(cat securities_table_schema.sql)"
-- Or copy and paste into ClickHouse client

CREATE TABLE IF NOT EXISTS price_db.vn_derivative_1d_hist  (
    symbol String,
    time DateTime64(3, 'Asia/Ho_Chi_Minh'),
    open Nullable(Float64),
    high Nullable(Float64),
    low Nullable(Float64),
    close Nullable(Float64),
    volume Nullable(Float64)
) ENGINE = ReplacingMergeTree()
PARTITION BY toYear(time)
ORDER BY (symbol, time)
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS price_db.vn_derivative_1m_hist (
    symbol String,
    time DateTime64(3, 'Asia/Ho_Chi_Minh'),
    open Nullable(Float64),
    high Nullable(Float64),
    low Nullable(Float64),
    close Nullable(Float64),
    volume Nullable(Float64)
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMMDD(time)
ORDER BY (symbol, time)
SETTINGS index_granularity = 8192;
-- To customize database and table name, modify above:
-- - Replace `default` with your database name
-- - Replace `securities_minute_data` with your table name

