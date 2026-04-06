# ReplacingMergeTree trong ClickHouse - Giải thích và Giải pháp

## Vấn đề: Tại sao ReplacingMergeTree không tự động replace?

**ReplacingMergeTree KHÔNG tự động deduplicate ngay khi INSERT!**

### Cách hoạt động:

1. **Khi INSERT**: Dữ liệu được ghi vào các "parts" riêng biệt, có thể có duplicate
2. **Background Merge**: ClickHouse tự động merge các parts theo thời gian → lúc này mới deduplicate
3. **Merge không diễn ra ngay**: Có thể mất vài phút đến vài giờ tùy cấu hình

## Giải pháp:

### 1. Sử dụng FINAL trong SELECT (Khuyến nghị cho query)

```sql
-- Xem kết quả đã deduplicate ngay lập tức
SELECT * FROM securities_minute_data FINAL
WHERE symbol = 'VN30F2410' AND date = '2024-10-01'
```

**Lưu ý**: FINAL sẽ chậm hơn vì phải merge trước khi query, nhưng đảm bảo kết quả chính xác.

### 2. Chạy OPTIMIZE TABLE ... FINAL (Để force merge ngay)

```sql
-- Force merge ngay để deduplicate
OPTIMIZE TABLE securities_minute_data FINAL;
```

**Khi nào dùng**: Sau khi insert xong batch lớn, muốn đảm bảo dữ liệu đã được deduplicate.

### 3. Cấu hình merge nhanh hơn (Tùy chọn)

```sql
-- Giảm thời gian chờ merge
ALTER TABLE securities_minute_data 
MODIFY SETTING merge_with_ttl_timeout = 3600;  -- 1 giờ thay vì mặc định
```

## Schema với ReplacingMergeTree:

Nếu bạn muốn chuyển sang ReplacingMergeTree, đây là schema mẫu:

```sql
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
    `date` Date,
    `_version` UInt64 DEFAULT now()  -- Cần cột version để xác định row nào mới hơn
)
ENGINE = ReplacingMergeTree(_version)  -- Row có _version lớn hơn sẽ được giữ lại
PARTITION BY toYYYYMMDD(date)
ORDER BY (symbol, timestamp)  -- Các row có cùng (symbol, timestamp) sẽ được deduplicate
SETTINGS index_granularity = 8192;
```

**Quan trọng**: 
- Cần cột `_version` (hoặc `_sign`, `_row_number`) để xác định row nào mới hơn
- ORDER BY phải bao gồm các cột định danh unique (ví dụ: symbol + timestamp)

## Best Practices:

1. **Luôn dùng FINAL khi query** nếu cần kết quả chính xác:
   ```sql
   SELECT * FROM table FINAL WHERE ...
   ```

2. **Chạy OPTIMIZE TABLE FINAL định kỳ** (ví dụ: sau mỗi batch insert lớn):
   ```sql
   OPTIMIZE TABLE securities_minute_data FINAL;
   ```

3. **Hoặc dùng cách hiện tại**: DELETE trước khi INSERT (như code đang làm)
   - Code hiện tại đã dùng `delete_by_symbol_and_date()` trước khi insert
   - Cách này đảm bảo không có duplicate nhưng tốn thời gian hơn

## So sánh với cách hiện tại:

**Cách hiện tại (DELETE + INSERT)**:
- ✅ Đảm bảo không có duplicate
- ✅ Không cần FINAL khi query
- ❌ Tốn thời gian hơn (phải DELETE trước)

**ReplacingMergeTree + FINAL**:
- ✅ INSERT nhanh hơn
- ✅ Tự động deduplicate khi merge
- ❌ Cần FINAL khi query (chậm hơn)
- ❌ Cần OPTIMIZE TABLE định kỳ
