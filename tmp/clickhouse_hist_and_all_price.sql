create table vn_corporate_actions
(
    symbol       String,
    ex_date      Date,
    cash_amount  Float64,
    bonus_rate   Float64,
    issue_price  Float64,
    issue_rate   Float64,
    ref_price    Float64,
    adjust_price Float64 default ((ref_price - cash_amount) + (issue_price * issue_rate)) /
                                 ((1 + bonus_rate) + issue_rate),
    adjust_rate  Float64 default adjust_price / ref_price,
    is_active    Bool    default true
)
engine = ReplacingMergeTree ORDER BY (symbol, ex_date)
SETTINGS index_granularity = 8192;

insert into price_db.vn_corporate_actions
select
    *
from default.vn_corporate_actions
;

create table vnstock_events
(
    symbol    String,
    timestamp UInt32,
    date Nullable(Date),
    event Nullable(String)
)
engine = ReplacingMergeTree ORDER BY (symbol, timestamp)
SETTINGS index_granularity = 8192;

insert into price_db.vnstock_events
select
    *
from default.vnstock_events;


-- DROP DICTIONARY vnstock_price_adjustment_dict
CREATE DICTIONARY vnstock_price_adjustment_dict
(
    symbol String,
    min_date Date,
    max_date Date,
    base_adjust_rate Float64,
    adjust_cum_rate Float64 DEFAULT 1.0
)
PRIMARY KEY symbol
SOURCE(CLICKHOUSE(
    QUERY '
        SELECT
            symbol,
            lag(ex_date) OVER (PARTITION BY symbol ORDER BY ex_date) AS min_date,
            (ex_date - interval ''1'' day) AS max_date,
            adjust_rate as base_adjust_rate,
            exp(sum(log(adjust_rate)) OVER (PARTITION BY symbol ORDER BY ex_date DESC)) AS adjust_cum_rate
        FROM vn_corporate_actions'
    USER 'clickhouse'
    PASSWORD 'PWjdDM@ANvZnule+'
    DB 'price_db'))
LAYOUT(COMPLEX_KEY_RANGE_HASHED())
RANGE(MIN min_date MAX max_date)
LIFETIME(MIN 600 MAX 1200);



CREATE VIEW vnstock_1d_adjusted_hist
(
    `symbol` String,
    `time` DateTime64(3, 'Asia/Ho_Chi_Minh'),
    `open` Nullable(Float64),
    `adjusted_open` Nullable(Float64),
    `high` Nullable(Float64),
    `adjusted_high` Nullable(Float64),
    `low` Nullable(Float64),
    `adjusted_low` Nullable(Float64),
    `close` Nullable(Float64),
    `adjusted_close` Nullable(Float64),
    `volume` Nullable(Float64)
 )
AS
SELECT v.symbol,
       time,
       open,
       v.open * dictGetOrDefault('default.vnstock_price_adjustment_dict', 'adjust_cum_rate', tuple(v.symbol), toDate(v.time), 1) AS adjusted_open,
       high,
       v.high * dictGetOrDefault('default.vnstock_price_adjustment_dict', 'adjust_cum_rate', tuple(v.symbol), toDate(v.time), 1) AS adjusted_high,
       low,
       v.low * dictGetOrDefault('default.vnstock_price_adjustment_dict', 'adjust_cum_rate', tuple(v.symbol), toDate(v.time), 1) AS adjusted_low,
       close,
       v.close * dictGetOrDefault('default.vnstock_price_adjustment_dict', 'adjust_cum_rate', tuple(v.symbol), toDate(v.time), 1) AS adjusted_close,
       volume
FROM price_db.vnstock_1d_hist AS v;


CREATE VIEW vnstock_1d_all
(
    `symbol` String,
    `time` DateTime64(3, 'Asia/Ho_Chi_Minh'),
    `open` Nullable(Float64),
    `high` Nullable(Float64),
    `low` Nullable(Float64),
    `close` Nullable(Float64),
    `volume` Nullable(Float64)
 )
AS
SELECT
    v.symbol,
    v.time,
    v.open * dictGetOrDefault('default.vnstock_price_adjustment_dict', 'adjust_cum_rate', tuple(v.symbol), toDate(v.time), 1) AS open,
    v.high * dictGetOrDefault('default.vnstock_price_adjustment_dict', 'adjust_cum_rate', tuple(v.symbol), toDate(v.time), 1) AS high,
    v.low * dictGetOrDefault('default.vnstock_price_adjustment_dict', 'adjust_cum_rate', tuple(v.symbol), toDate(v.time), 1) AS low,
    v.close * dictGetOrDefault('default.vnstock_price_adjustment_dict', 'adjust_cum_rate', tuple(v.symbol), toDate(v.time), 1) AS close,
    volume
FROM price_db.vnstock_1d_hist AS v
where toDate(time) < toDate(current_timestamp(), 'Asia/Ho_Chi_Minh')
union all
SELECT
    symbol,
    time,
    argMinMerge(open) as open,
    maxMerge(high) as high,
    minMerge(low) as low,
    argMaxMerge(close) as close,
    sumMerge(volume) as volume
FROM price_db.vnstock_1d_realtime
where toDate(time) > toDate(current_timestamp(), 'Asia/Ho_Chi_Minh')
GROUP BY symbol, time