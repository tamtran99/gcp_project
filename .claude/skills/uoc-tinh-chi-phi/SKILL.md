---
name: uoc-tinh-chi-phi
description: Ước tính số bytes/GB một query hoặc model dbt sẽ quét trên BigQuery TRƯỚC khi chạy, bằng dry run (miễn phí), và đọc chi phí thật sau khi chạy từ run_results.json. Dùng khi được hỏi query này tốn bao nhiêu, có đắt không, làm sao giảm chi phí BigQuery, hoặc trước khi chạy một model nặng.
---

# Ước tính chi phí query BigQuery

BigQuery on-demand tính tiền theo **lượng dữ liệu quét**, không theo thời gian chạy.
Có ba mức đo, dùng đúng mức cho đúng tình huống.

## Mức 1 — Dry run: biết TRƯỚC khi chạy, miễn phí

Dry run trả về số bytes sẽ quét mà không thực thi và không tính tiền.

```powershell
dbt compile --select <model>
python .claude/skills/uoc-tinh-chi-phi/scripts/estimate_scan.py --select <model>
```

Bỏ `--select` để quét toàn bộ model đã compile.

### Bốn cái bẫy phải nói rõ khi báo cáo kết quả

1. **Model incremental bị ước tính THẤP.** SQL trong `target/compiled/` chỉ là phần
   `SELECT`. Lúc chạy thật dbt bọc thêm `MERGE`, mà MERGE còn quét cả bảng đích.
   Muốn đo đúng thì lấy SQL từ `target/run/` — script có cờ `--run` cho việc này.
2. **Dry run fail nếu bảng cha chưa tồn tại.** Chỉ dry-run được model có upstream đã build.
3. **Model `view` luôn ra 0 bytes.** Tạo view không quét gì, nhưng chi phí không biến mất —
   nó chuyển sang người query view đó. Đừng báo cáo "view miễn phí".
4. **Lần chạy đầu và lần incremental compile ra SQL khác nhau** (`is_incremental()` chỉ
   true khi bảng đã tồn tại).

## Mức 2 — Log lúc chạy

`dbt-bigquery` in bytes ngay trong output, xem `logs/dbt.log`:
```
[CREATE TABLE (41.0 rows, 1.3 MB processed) in 4.2s]
```

## Mức 3 — `run_results.json`: số thật sau khi chạy

Mỗi model có `adapter_response` chứa `bytes_processed`, `bytes_billed`, `slot_ms`, `job_id`.
Đây là bằng chứng before/after chính xác nhất khi chứng minh một tối ưu có hiệu quả.

```powershell
python .claude/skills/uoc-tinh-chi-phi/scripts/estimate_scan.py --actual
```

## Điều quan trọng nhất: `bytes_billed` ≠ `bytes_processed`

BigQuery on-demand tính **tối thiểu 10 MB cho mỗi bảng được tham chiếu**, làm tròn lên.
Lần chạy gần nhất của repo này: 7 MB processed nhưng **125 MB billed** — gấp 18 lần.

Hệ quả khi tư vấn:
- Luôn quy ra tiền bằng `bytes_billed`, không phải `bytes_processed`
- Project có hàng trăm model nhỏ sẽ tốn hơn nhiều so với con số processed gợi ý
- Model chạy 5 phút/lần quét vài KB vẫn tốn 10 MB mỗi lần

## Hàng rào, không chỉ đo

Khi không đoán được trước, đừng cố đoán — đặt trần để vượt ngưỡng thì **fail ồn ào và
miễn phí**: bỏ comment `maximum_bytes_billed` trong [profiles.yml](profiles.yml).
Job vượt trần bị huỷ ngay, không bị tính tiền.
