# `airflow_config/` — khai báo DAG bằng YAML

Mỗi file `.yml` trong thư mục này khai báo một hoặc nhiều DAG.
`dags/dag_builder.py` quét thư mục, validate rồi sinh DAG động.

**Thêm một pipeline mới = thêm một khối YAML. Không viết Python.**

## Schema

```yaml
<dag_id>:                        # khoá mức cao nhất CHÍNH LÀ dag_id
  is_disabled: false             # true -> không tạo DAG (xem ghi chú bên dưới)
  config:
    select:                      # BẮT BUỘC — danh sách selector dbt
      - path:models/marts
      - tag:daily
    exclude: []                  # tuỳ chọn — selector loại trừ
    dag_params:                  # tham số truyền thẳng vào DAG(...)
      tags: [...]
      schedule: "0 * * * *"
      catchup: false
      default_args:
        owner: "corpGroup:Data-Platform"
```

Các selector trong `select` được gộp thành **union** (dbt hiểu nhiều selector
cách nhau bằng dấu cách trong một `--select` là phép hợp).

### Hai dạng `schedule`

```yaml
schedule: "15 * * * *"           # chuỗi cron trần, timezone lấy theo mặc định
```
```yaml
schedule:                        # object — ghim luôn timezone
  cron: "0 2 * * *"
  timezone: Asia/Ho_Chi_Minh
```

Cả hai đều cho ra `CronTriggerTimetable`. Dạng object chỉ khác ở chỗ nói rõ
timezone thay vì đi mượn của DAG. Dùng dạng object cho mọi job có ràng buộc giờ
làm việc Việt Nam.

### `is_disabled`

`is_disabled: true` khiến DAG **không được tạo** chứ không phải tạo rồi pause.
Lý do: `is_paused_upon_creation` chỉ có tác dụng lần serialize đầu tiên, nên bật
cờ trên một DAG đã tồn tại và đang chạy sẽ không dừng được nó — đúng kiểu sự cố
âm thầm. Bỏ hẳn DAG mới là idempotent.

Lịch sử chạy của DAG bị bỏ vẫn được giữ; Airflow đánh dấu inactive và ẩn khỏi bộ
lọc mặc định. Bật lại thì lịch sử còn nguyên.

## Khoá mở rộng (không có trong schema gốc)

Đều đọc bằng `.get()` nên file YAML chỉ có `select` + `dag_params` vẫn parse
nguyên vẹn.

```yaml
config:
  pre_tasks:                     # script Python chạy trước dbt, tuần tự
    - name: load_to_bigquery
      script: scripts/load_to_bigquery.py

  dbt:
    target: prod                 # mặc định prod
    command: build               # build | run | test | snapshot | seed
    exclude: []
    vars: {}                     # truyền vào --vars
    full_refresh: false
    split_run_test: false        # true -> tách dbt_run >> dbt_test
    extra_args: []               # cờ dbt thô, nối vào cuối lệnh
```

`split_run_test` mặc định `false` là có chủ ý. `dbt build` xen kẽ theo từng
node — nó test `stg_ecommerce__orders` **trước** khi build `fct_orders` từ model
đó. Tách `run` rồi mới `test` thì dữ liệu bẩn đã kịp merge vào `fct_orders`
(incremental), gỡ ra phải `--full-refresh`.

## Khoá KHÔNG dùng được

| Khoá | Lý do |
|---|---|
| `default_args.sla`, `sla_miss_callback` | Airflow 3 đã gỡ SLA — dùng Deadline Alerts |
| `schedule_interval` | Airflow 3 đổi tên thành `schedule` |

Factory reject tường minh kèm thông báo, không im lặng bỏ qua.

## Kiểm tra trước khi push

```bash
source ~/airflow/airflow-env.sh
~/venvs/airflow/bin/python "$GCP_REPO_DIR/dags/dag_builder.py"   # liệt kê DAG dựng được
~/venvs/airflow/bin/airflow dags list-import-errors              # phải rỗng
```

File YAML sai cú pháp **không** làm mất các DAG khác: mỗi file hỏng sinh ra một
DAG `_config_error__<tên>` mang tag `config-error`, task của nó fail kèm thông
báo lỗi. Nhìn UI là thấy ngay.

## Sau khi push

`git_sync_main` kéo `origin/main` về mỗi 5 phút rồi chạy
`airflow dags reserialize`. Thay đổi trong `airflow_config/*.yml` lên UI sau
tối đa ~5 phút, không cần restart gì cả.
