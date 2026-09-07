---
description: Soát đồ thị phụ thuộc dbt, tìm model không ai dùng
---

Phân tích đồ thị phụ thuộc của project và báo cáo sức khoẻ cấu trúc.

Đọc `target/manifest.json` (chạy `dbt compile` trước nếu file cũ) và trả lời:

1. **Đồ thị hiện tại** — mỗi model có bao nhiêu model downstream, phân theo tầng
2. **Model đầu cuối không có exposure** — không model nào dùng, cũng không khai báo
   exposure nào trỏ tới. Đây là ứng viên bảng chết
3. **Vi phạm phân tầng** — mart join thẳng vào source bỏ qua staging; staging phụ thuộc
   vào mart (chiều ngược); model intermediate `ephemeral` bị nhiều mart dùng lại
4. **Model fanout** — một model có quá nhiều con trực tiếp, dấu hiệu thiếu tầng trung gian

Script [scripts/sync_exposures.py](scripts/sync_exposures.py) đã làm sẵn phần đối chiếu
manifest với log truy vấn BigQuery — chạy `--dry-run` trước.

**Quan trọng khi kết luận:** DAG của dbt dừng ở mart. Một mart không có model downstream
KHÔNG có nghĩa là nó chết — dashboard có thể đang đọc nó mỗi ngày. Chỉ kết luận "chết"
khi có thêm bằng chứng từ `INFORMATION_SCHEMA.JOBS` là 90 ngày không ai query.
