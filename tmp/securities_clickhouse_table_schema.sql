-- ClickHouse table schema for securities minute data
-- Run this SQL to create the table before using the DAG
-- 
-- Usage:
--   clickhouse-client --query "$(cat securities_table_schema.sql)"
-- Or copy and paste into ClickHouse client

CREATE TABLE IF NOT EXISTS `default`.`securities_minute_data`
(
    `symbol` String,
    `timestamp` UInt32,
    `datetime` DateTime,
    `open` Nullable(Float64),
    `high` Nullable(Float64),
    `low` Nullable(Float64),
    `close` Nullable(Float64),
    `volume` Nullable(Float64),
    `date` Date
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(date)
ORDER BY (symbol, timestamp)
SETTINGS index_granularity = 8192;

-- To customize database and table name, modify above:
-- - Replace `default` with your database name
-- - Replace `securities_minute_data` with your table name

