# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Repo và toàn bộ comment/tài liệu viết bằng **tiếng Việt** — giữ nguyên ngôn ngữ đó khi thêm code mới.

## Tổng quan

Pipeline analytics: **CSV → BigQuery (landing) → dbt transform → marts cho BI**.
dbt Core 1.12 + `dbt-bigquery`, chạy trên Windows/PowerShell.

## Nạp môi trường — bắt buộc trước mọi lệnh

`profiles.yml` và `models/staging/_src_ecommerce.yml` đọc cấu hình qua `env_var()`,
nên **mọi lệnh dbt/Python đều fail nếu chưa nạp `.env`**:

```powershell
.\.venv\Scripts\Activate.ps1
. .\scripts\load_env.ps1        # PHẢI dot-source (dấu chấm + khoảng trắng)
```

`load_env.ps1` làm 3 việc ngoài việc đọc `.env`: quy `DBT_PROFILES_DIR` về đường dẫn
tuyệt đối của repo, set `PYTHONUTF8=1`, và thêm `gcloud` vào PATH của session.

`PYTHONUTF8=1` là bắt buộc, không phải tuỳ chọn: Windows tiếng Việt đọc file bằng
codepage cp1258, thiếu biến này dbt sẽ `UnicodeDecodeError` khi gặp tiếng Việt
trong `.sql`/`.yml`.

Xác thực dùng ADC (`gcloud auth application-default login`) — profile `dev` là
`method: oauth`, không dùng service account key ở local.

## Lệnh thường dùng

```powershell
dbt deps                                 # BẮT BUỘC trước lần build đầu, nếu không test dbt_utils lỗi "macro not found"
dbt debug                                # kiểm tra kết nối
dbt build                                # run + test theo đúng thứ tự phụ thuộc
dbt build --select <model>               # vòng lặp phát triển — dùng cái này, đừng build cả project
dbt run --select +fct_orders             # model + upstream
dbt run --select fct_orders+             # model + downstream
dbt run --full-refresh                   # build lại bảng incremental từ đầu
dbt test --select stg_ecommerce__orders  # chỉ test của 1 model
dbt source freshness
dbt docs generate; dbt docs serve

sqlfluff lint models/                    # lint SQL (templater = dbt)

python scripts\generate_sample_data.py   # sinh CSV mẫu vào data/raw/
python scripts\setup_bigquery.py         # tạo dataset raw_ecommerce + dbt_dev
python scripts\load_to_bigquery.py       # CSV -> BigQuery (WRITE_TRUNCATE, idempotent)
```

VS Code Tasks trong `.vscode/tasks.json` đã bọc sẵn `load_env.ps1` cho các lệnh trên.

## Kiến trúc

```
data/raw/*.csv
  → raw_ecommerce.{customers,products,orders,order_items}   ← landing, KHÔNG sửa
  → staging/       stg_ecommerce__*        view
  → intermediate/  int_order_items_enriched
  → marts/         dim_customers, dim_products, fct_orders,
                   fct_order_items, agg_daily_revenue        ← BI query ở đây
```

**Quy tắc phân tầng:**
- `staging/` — một model ↔ một bảng nguồn. Chỉ đổi tên, ép kiểu, làm sạch
  (`lower(trim(email))`, `nullif(trim(city), '')`). **Không join, không logic nghiệp vụ.**
- `intermediate/` — mặc định `ephemeral` trong `dbt_project.yml`.
  `int_order_items_enriched` **cố ý ghi đè thành `table`** vì được 4 mart dùng lại;
  để ephemeral thì BigQuery join lại 3 bảng 4 lần = trả tiền quét 4 lần.
  Đây không phải nhầm lẫn — cân nhắc đánh đổi tương tự khi thêm model intermediate mới.
- `marts/` — `table`, partition theo `order_date`, cluster theo cột hay lọc.

**Grain của từng mart:** `dim_customers` = 1 khách · `dim_products` = 1 sản phẩm ·
`fct_orders` = 1 đơn · `fct_order_items` = 1 dòng hàng · `agg_daily_revenue` = 1 ngày.

## Quy ước cần biết trước khi sửa code

**Cú pháp test dbt 1.12** — tham số của generic test phải lồng dưới `arguments:`,
cú pháp phẳng kiểu cũ sẽ lỗi:

```yaml
- relationships:
    arguments:
      to: ref('stg_ecommerce__orders')
      field: order_id
```

**NUMERIC ≠ FLOAT64.** BigQuery không tự ép kiểu, nên `unit_price / 100.0` sẽ lỗi
`No matching signature for operator *`. Giữ mọi phép tính tiền trong NUMERIC:
`unit_price * quantity * (100 - discount_pct) / 100`.

**`fct_orders` là incremental với cửa sổ lùi 7 ngày**, không phải đúng `max(order_date)` —
trạng thái đơn đổi muộn (`pending → shipped → completed`) nên phải quét lại cửa sổ đó.
Sửa logic model này thì cần `--full-refresh`.

**Đặt tên dataset** do `macros/generate_schema_name.sql` quyết định:
dev → `dbt_dev_staging`, `dbt_dev_marts`; prod → `staging`, `marts`.
Mỗi dev đặt `BQ_DBT_DATASET` riêng trong `.env` để không đè lên nhau.

**Ngưỡng dùng chung** khai báo ở `vars` trong `dbt_project.yml`
(`start_date`, `churn_threshold_days`) — gọi bằng `var()`, đừng hardcode trong model.

**Dữ liệu mẫu trải 24 tháng gần nhất tính từ `current_date()`**, không phải mốc cố định.
`dim_customers` phân khúc churn bằng `date_diff(current_date(), last_order_date)`, nên
cắm ngày cứng sẽ khiến toàn bộ khách rơi vào `churned` sau vài tháng.

**Style SQL** (`.sqlfluff`): keyword/identifier/function viết thường, alias phải tường minh
(`as`), max 88 ký tự, CTE và JOIN không thụt lề thêm.

## Orchestration bằng Airflow

Airflow chạy trong **WSL2 Ubuntu**, không phải Windows (Airflow không hỗ trợ
Windows native). Hai venv **tách biệt**: `~/venvs/airflow` và `~/venvs/dbt` —
dbt-core 1.12 và airflow-core ghim `jinja2`/`click`/`protobuf` ở các khoảng
xung đột nhau. Đó cũng là lý do task dbt là `BashOperator` gọi subprocess chứ
không phải `PythonOperator` import dbt.

```bash
# trong WSL
bash scripts/airflow_bootstrap.sh              # cài đặt (chạy lại được nhiều lần)
bash scripts/airflow_bootstrap.sh --start      # bật UI ở localhost:8080
bash scripts/airflow_bootstrap.sh --sync-dags  # cập nhật DAG bootstrap
```

### Thêm pipeline = thêm YAML, không viết Python

`dags/dag_builder.py` quét `airflow_config/*.yml` và sinh DAG động. Mỗi khoá mức
cao nhất là một `dag_id`. Chi tiết schema: `airflow_config/README.md`.

```yaml
<dag_id>:
  is_disabled: false
  config:
    select: [path:models/marts]      # selector dbt, gộp thành union
    dag_params:
      schedule: "15 * * * *"          # hoặc {cron: ..., timezone: ...}
      tags: [...]
      default_args: {owner: "..."}
```

Kiểm tra trước khi push: `python dags/dag_builder.py` (liệt kê DAG dựng được) và
`airflow dags list-import-errors`.

**File YAML hỏng không làm mất DAG khác** — mỗi file lỗi sinh ra một DAG
`_config_error__<tên>` mang tag `config-error`, task fail kèm thông báo.

**`is_disabled: true` là không tạo DAG**, không phải tạo rồi pause. Cố ý:
`is_paused_upon_creation` chỉ có tác dụng lần serialize đầu tiên nên bật cờ trên
DAG đang chạy sẽ không dừng được nó.

### Ba cái bẫy khi sửa phần Airflow

**`git clean -fd` — không bao giờ thêm `-x`.** `airflow_bootstrap/git_sync_main.py`
reset clone về `origin/main` mỗi 5 phút. `-x` sẽ xoá luôn file bị `.gitignore`:
`dbt_packages/`, `target/`, `data/raw/*.csv`.

**Pool `gcp_project_repo` là mutex, không phải throttle.** Task dbt lấy 1 slot;
`git reset` và `dbt deps` lấy TOÀN BỘ 8 slot. Bỏ `pool_slots` đi thì `git reset
--hard` sẽ đổi file `.sql` giữa lúc dbt đang đọc — cho ra lần chạy nửa cũ nửa mới,
gần như không tái hiện được.

**PR build phải dùng `--target ci`, tuyệt đối không phải `prod`.**
`macros/generate_schema_name.sql` trả tên schema THÔ khi `target.name == 'prod'`,
nên `--target prod` từ nhánh PR sẽ đè thẳng lên bảng production. Với target `ci`
macro thêm tiền tố `BQ_CI_DATASET` → PR #42 ghi vào `pr_42_marts`.

### Vài chi tiết khác

- **Hai bundle DAG.** `bootstrap` (`~/airflow/dags_bootstrap/`, nằm NGOÀI repo) và
  `gcp-project` (`<clone>/dags`). DAG git-sync ở bundle bootstrap và được **copy**
  chứ không symlink — nếu nó nằm trong thư mục bị `git reset` thì một commit hỏng
  trên main có thể vô hiệu hoá luôn cơ chế kéo code về để sửa.
- **`min_file_process_interval=30`** khiến dag-processor parse lại bất kể mtime.
  Bắt buộc: sửa `airflow_config/*.yml` không làm đổi mtime của `dag_builder.py`.
- **`DBT_PROFILES_DIR` phải tuyệt đối** trong `~/airflow/airflow-env.sh`. `.env` của
  repo đặt `=.` mà BashOperator chạy ở thư mục tạm → "Could not find profile".
- **Không dùng service account key — project bật `iam.disableServiceAccountKeyCreation`.**
  Tải key về sẽ lỗi `FAILED_PRECONDITION`. Target `prod`/`ci` dùng `method: oauth`
  cộng `impersonate_service_account` (`GCP_IMPERSONATE_SA`): xác thực bằng ADC rồi
  mượn danh tính SA. Cần `roles/iam.serviceAccountTokenCreator` trên chính SA đó và
  API `iamcredentials.googleapis.com` đã bật. Grant IAM mất ~40 giây mới có hiệu lực —
  `PERMISSION_DENIED` ngay sau khi cấp quyền thường chỉ là chưa lan truyền.
- **`GCP_IMPERSONATE_SA` để trống là hợp lệ, không phải quên.** dbt render TOÀN BỘ
  `profiles.yml` nên target `dev` cũng phải parse được dòng đó; chuỗi rỗng được dbt
  chuẩn hoá thành `None` = không mượn danh tính ai.
- **`scripts/*.py` KHÔNG mượn danh tính SA.** Chúng dùng `bigquery.Client()` nên chạy
  dưới tài khoản trong `GOOGLE_APPLICATION_CREDENTIALS`, còn dbt thì mượn SA. Hai
  danh tính khác nhau — quyền lệch nhau sẽ ra lỗi chỉ xuất hiện ở một bên.
- **`append_env=True`** trên mọi BashOperator. Mặc định `False` thay sạch
  environment, mất `PATH`/`HOME` và mọi `env_var()` trong `profiles.yml` sẽ nổ.
- **Mỗi task dbt có `--target-path` riêng** (`target/airflow/<dag>/<ts>_<try>`).
  Dùng chung `target/` thì hai lần chạy chồng nhau ghi đè `manifest.json` và
  `partial_parse.msgpack` của nhau.
- **Airflow 3 khác 2:** `airflow api-server` (không phải `webserver`), `db migrate`
  (không phải `db init`), `schedule` (không phải `schedule_interval`), SLA đã bị gỡ,
  `BashOperator` nằm ở `airflow.providers.standard.operators.bash`.

## Chi phí BigQuery

Tính tiền theo lượng dữ liệu quét. Luôn lọc theo cột partition `order_date`;
dùng `--select` thay vì build cả project; `maximum_bytes_billed` trong `profiles.yml`
đang comment, bỏ comment khi cần chặn cứng ở 20 GB.
