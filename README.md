# Ecommerce Analytics — BigQuery + dbt Core

Repo mẫu cho pipeline phân tích dữ liệu:
**CSV → BigQuery (landing) → dbt transform → marts phục vụ BI.**

| Thành phần | Công nghệ |
|---|---|
| Data warehouse | Google BigQuery |
| Transformation | dbt Core + adapter `dbt-bigquery` |
| Nạp dữ liệu | Python (`google-cloud-bigquery`) |
| IDE | VS Code |

---

## Cấu trúc repo

```
gcp_project/
├── .env.example              # Mẫu biến môi trường — copy thành .env
├── profiles.yml              # Kết nối dbt -> BigQuery (đọc từ env, an toàn để commit)
├── dbt_project.yml           # Cấu hình project dbt
├── packages.yml              # Package dbt phụ thuộc (dbt_utils, dbt_expectations)
├── requirements.txt          # Thư viện Python
│
├── .vscode/                  # Cấu hình VS Code + Tasks chạy 1 click
│
├── scripts/
│   ├── load_env.ps1          # Nạp .env vào session PowerShell
│   ├── _env.py               # Đọc cấu hình dùng chung
│   ├── generate_sample_data.py   # Sinh CSV mẫu vào data/raw/
│   ├── setup_bigquery.py     # Kiểm tra kết nối + tạo dataset
│   └── load_to_bigquery.py   # Nạp CSV -> BigQuery
│
├── data/raw/                 # CSV nguồn (bị gitignore)
│
├── models/
│   ├── staging/              # Làm sạch 1-1 với bảng nguồn -> view
│   ├── intermediate/         # Bước trung gian -> ephemeral
│   └── marts/                # Bảng cho BI -> table
│
├── macros/                   # Macro Jinja dùng lại
├── tests/                    # Singular test viết tay
└── analyses/                 # Query khảo sát (compile, không tạo bảng)
```

### Luồng dữ liệu

```
data/raw/*.csv
      │  scripts/load_to_bigquery.py
      ▼
raw_ecommerce.{customers, products, orders, order_items}      ← landing, không sửa
      │  dbt
      ▼
staging   stg_ecommerce__*          view       ← làm sạch, ép kiểu, đổi tên
      │
      ▼
intermediate  int_order_items_enriched   ephemeral   ← join 3 bảng, tính lợi nhuận
      │
      ▼
marts     dim_customers, dim_products     table
          fct_orders (incremental), fct_order_items
          agg_daily_revenue                          ← dashboard đọc ở đây
```

---

## Bước 1 — Chuẩn bị môi trường trong VS Code

### 1.1. Phần mềm cần có

| Phần mềm | Kiểm tra |
|---|---|
| Python 3.11 (bản **x64**) | `python --version` |
| Git | `git --version` |
| Google Cloud CLI | `gcloud --version` |

> **Lưu ý cho máy Windows ARM:** hãy cài Python **x64** (không phải arm64).
> Nhiều thư viện phụ thuộc của dbt chưa có bản build sẵn cho `win_arm64`;
> bản x64 chạy qua lớp giả lập vẫn ổn định và cài nhanh hơn nhiều.

Cài bằng winget nếu chưa có:

```powershell
winget install --id Python.Python.3.11 --architecture x64 --source winget
winget install --id Git.Git --source winget
winget install --id Google.CloudSDK --source winget
```

Sau khi cài xong phải **mở lại terminal** thì PATH mới cập nhật.

### 1.2. Tạo virtual environment

```powershell
cd C:\Users\tranc\Documents\gcp_project

python -m venv .venv
.\.venv\Scripts\Activate.ps1

python -m pip install --upgrade pip
pip install -r requirements.txt
```

Nếu PowerShell chặn script activate:

```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

### 1.3. Extension VS Code

Mở thư mục này bằng VS Code rồi bấm **Install All** khi nó gợi ý
extension (danh sách nằm ở `.vscode/extensions.json`):

- **Python** — chọn interpreter `.venv`
- **dbt Power User** — autocomplete `ref()`, xem lineage, preview kết quả model
- **Jinja HTML / YAML** — highlight cú pháp `{{ }}` trong file `.sql`

Chọn interpreter: `Ctrl+Shift+P` → *Python: Select Interpreter* → chọn
`.\.venv\Scripts\python.exe`.

### 1.4. Tạo file `.env`

```powershell
Copy-Item .env.example .env
```

Mở `.env`, điền `GCP_PROJECT_ID` của bạn và chọn `BQ_LOCATION` phù hợp
(ví dụ `asia-southeast1` cho Singapore, `US` cho multi-region Mỹ).

Mỗi khi mở terminal mới, nạp biến vào session:

```powershell
. .\scripts\load_env.ps1      # chú ý dấu chấm + khoảng trắng ở đầu
```

---

## Bước 2 — Cài đặt dbt

Đã nằm trong `requirements.txt` ở bước 1.2. Kiểm tra:

```powershell
dbt --version
```

Kết quả cần thấy `Core: 1.9.x` và `bigquery: 1.9.x`.

Cài các package dbt phụ thuộc (`dbt_utils`, `dbt_expectations`):

```powershell
dbt deps
```

Lệnh này tải package vào `dbt_packages/` (đã gitignore). **Bắt buộc chạy
trước `dbt build`**, nếu không các test `dbt_utils.accepted_range` sẽ lỗi
"macro not found".

---

## Bước 3 — Kết nối Google Cloud

### 3.1. Đăng nhập

```powershell
gcloud auth login                          # đăng nhập tài khoản Google
gcloud config set project <GCP_PROJECT_ID> # chọn project mặc định
gcloud auth application-default login      # tạo credential cho thư viện client
```

Lệnh thứ ba là quan trọng nhất: nó tạo **Application Default Credentials
(ADC)** — cả script Python lẫn dbt (`method: oauth`) đều dùng file này,
nên không cần tải service account key về máy.

File ADC nằm ở:
`%APPDATA%\gcloud\application_default_credentials.json`

### 3.2. Bật API và cấp quyền

```powershell
gcloud services enable bigquery.googleapis.com
```

Tài khoản của bạn cần tối thiểu 2 role trên project:

| Role | Dùng để |
|---|---|
| `roles/bigquery.dataEditor` | Tạo/ghi dataset và bảng |
| `roles/bigquery.jobUser` | Chạy query và load job |

Nếu bạn là Owner của project thì đã có sẵn.

### 3.3. Tạo dataset + kiểm tra kết nối

```powershell
. .\scripts\load_env.ps1
python scripts\setup_bigquery.py
```

Script tạo 2 dataset nếu chưa có:
- `raw_ecommerce` — vùng đổ dữ liệu thô
- `dbt_dev` — dataset đích của dbt

Sau đó kiểm tra dbt kết nối được chưa:

```powershell
dbt debug
```

Cần thấy `All checks passed!`.

---

## Bước 4 — Load dữ liệu lên GCP

### 4.1. Sinh dữ liệu mẫu

```powershell
python scripts\generate_sample_data.py
```

Tạo 4 file trong `data/raw/`: 500 khách hàng, 60 sản phẩm, 4.000 đơn
hàng và khoảng 12.000 dòng hàng. Dữ liệu cố ý có "rác" (email viết hoa,
khoảng trắng thừa, ô rỗng, đơn bị huỷ) để tầng staging có việc làm thật.

> Muốn dùng dữ liệu thật của bạn: bỏ file CSV vào `data/raw/` và sửa
> khai báo schema trong `scripts/load_to_bigquery.py` cho khớp.

### 4.2. Nạp lên BigQuery

```powershell
python scripts\load_to_bigquery.py
```

Điểm đáng chú ý trong script:

- **Schema tường minh** thay vì để BigQuery tự đoán — tự đoán rất hay
  nhầm kiểu khi cột có ô rỗng.
- **`WRITE_TRUNCATE`** — chạy lại bao nhiêu lần cũng ra cùng kết quả.
- **Partition + cluster** — `orders` partition theo `order_date` và
  cluster theo `customer_id`, giúp query lọc theo ngày quét ít dữ liệu
  hơn hẳn, tức là rẻ hơn.

Kiểm tra lại trên BigQuery:

```powershell
bq query --use_legacy_sql=false "select count(*) from ``$env:GCP_PROJECT_ID.raw_ecommerce.orders``"
```

---

## Bước 5 — Viết transform với dbt

### 5.1. Chạy toàn bộ pipeline

```powershell
dbt build
```

`dbt build` = `dbt run` + `dbt test`, chạy theo đúng thứ tự phụ thuộc và
dừng nhánh nào có test fail — an toàn hơn chạy `run` rồi `test` riêng.

Các lệnh hay dùng khác:

```powershell
dbt run                                  # chỉ tạo bảng/view
dbt test                                 # chỉ chạy test
dbt run --select stg_ecommerce__orders   # chạy đúng 1 model
dbt run --select +fct_orders             # model đó và mọi thứ nó phụ thuộc
dbt run --select fct_orders+             # model đó và mọi thứ phụ thuộc vào nó
dbt run --full-refresh                   # build lại bảng incremental từ đầu
dbt docs generate; dbt docs serve        # mở trang tài liệu + sơ đồ lineage
```

### 5.2. Ba tầng model và lý do phân tầng

**`models/staging/`** — mỗi model tương ứng đúng một bảng nguồn.
Chỉ đổi tên cột, ép kiểu, làm sạch. **Không join, không tính nghiệp vụ.**
Materialize là `view` nên không tốn dung lượng lưu trữ.

Ví dụ trong `stg_ecommerce__customers.sql`:

```sql
lower(trim(email))          as email,   -- "  AN@X.COM " -> "an@x.com"
nullif(trim(city), '')      as city     -- ô rỗng phải là NULL, không phải ''
```

**`models/intermediate/`** — bước trung gian phức tạp, ở đây là join
order_items × orders × products và tính lợi nhuận gộp. Materialize
`ephemeral`: dbt chèn thẳng SQL này thành CTE trong model cha, không tạo
đối tượng nào trên BigQuery.

**`models/marts/`** — bảng cuối cho BI, materialize `table`:

| Model | Hạt (grain) | Vai trò |
|---|---|---|
| `dim_customers` | 1 khách hàng | Thuộc tính + LTV + phân khúc vòng đời |
| `dim_products` | 1 sản phẩm | Thuộc tính + doanh thu + xếp hạng ngành |
| `fct_orders` | 1 đơn hàng | Fact chính, **incremental** |
| `fct_order_items` | 1 dòng hàng | Fact chi tiết nhất |
| `agg_daily_revenue` | 1 ngày | Bảng nhỏ cho dashboard |

### 5.3. Vài kỹ thuật đáng chú ý trong repo

**Model incremental** (`fct_orders.sql`) — chỉ xử lý phần dữ liệu mới
thay vì build lại toàn bộ mỗi lần:

```sql
{% if is_incremental() %}
where order_date >= (
    select date_sub(max(order_date), interval 7 day) from {{ this }}
)
{% endif %}
```

Lùi 7 ngày thay vì lấy đúng `max(order_date)` vì trạng thái đơn có thể
đổi muộn (`pending` → `shipped` → `completed`), cần quét lại cửa sổ đó
mới bắt được thay đổi.

**Biến dùng chung** khai báo ở `dbt_project.yml`, gọi bằng `var()`:

```sql
when date_diff(current_date(), last_order_date, day)
     > {{ var('churn_threshold_days') }} then 'churned'
```

Đổi ngưỡng churn chỉ cần sửa một chỗ.

**Cẩn thận kiểu NUMERIC.** BigQuery không tự ép `NUMERIC` với `FLOAT64`,
nên `unit_price / 100.0` sẽ lỗi. Viết như trong
`stg_ecommerce__order_items.sql` để mọi phép tính tiền ở lại trong
NUMERIC, tránh sai số thập phân khi cộng dồn:

```sql
unit_price * quantity * (100 - discount_pct) / 100  as net_amount
```

**Đặt tên dataset theo môi trường** (`macros/generate_schema_name.sql`):

| Target | Dataset thực tế |
|---|---|
| `dev` | `dbt_dev_staging`, `dbt_dev_marts` |
| `prod` | `staging`, `marts` |

Nhờ vậy mỗi dev build vào dataset riêng, không đè lên nhau.

### 5.4. Kiểm thử dữ liệu

Repo có 3 loại test:

1. **Test tích hợp sẵn** khai báo trong file `_*.yml`: `unique`,
   `not_null`, `accepted_values`, `relationships`.
2. **Test từ package**: `dbt_utils.accepted_range` để chặn giá trị âm
   hoặc phần trăm vượt 100.
3. **Singular test** viết tay ở `tests/`:
   `assert_order_revenue_matches_items.sql` đối chiếu doanh thu ở
   `fct_orders` với tổng dòng hàng ở `fct_order_items` — bắt lỗi rollup
   mà các test cột thông thường không thấy được.

```powershell
dbt test                                    # toàn bộ
dbt test --select stg_ecommerce__orders     # test của 1 model
```

### 5.5. Thêm một model mới

1. Tạo file `.sql` trong thư mục tầng phù hợp.
2. Dùng `{{ ref('model_khac') }}` và `{{ source('ecommerce', 'bang') }}`
   thay cho tên bảng cứng — đây là cách dbt tự dựng đồ thị phụ thuộc.
3. Khai báo model + test trong file `_*.yml` cùng thư mục.
4. Chạy `dbt build --select ten_model_moi`.

---

## Xử lý lỗi thường gặp

| Lỗi | Nguyên nhân & cách xử lý |
|---|---|
| `Env var required but not provided: 'GCP_PROJECT_ID'` | Chưa nạp `.env`. Chạy `. .\scripts\load_env.ps1` |
| `Could not automatically determine credentials` | Chạy `gcloud auth application-default login` |
| `Access Denied: Project ...` | Thiếu role `bigquery.jobUser` / `bigquery.dataEditor` |
| `Dataset was not found in location EU` | `BQ_LOCATION` trong `.env` không khớp region thật của dataset |
| `Compilation Error: macro 'accepted_range' not found` | Chưa chạy `dbt deps` |
| `No matching signature for operator * (NUMERIC, FLOAT64)` | Đang chia cho `100.0`. Xem mục 5.3 về NUMERIC |
| `dbt: command not found` | Chưa activate venv: `.\.venv\Scripts\Activate.ps1` |
| Profile not found | Đặt `DBT_PROFILES_DIR` về thư mục repo (đã có trong `load_env.ps1`) |

---

## Kiểm soát chi phí BigQuery

BigQuery tính tiền theo **lượng dữ liệu quét**, không theo thời gian chạy.

- Bỏ comment `maximum_bytes_billed` trong `profiles.yml` để chặn cứng
  query quét quá 20 GB.
- Luôn lọc theo cột partition (`order_date`) — đó là lý do các bảng fact
  được partition.
- `dbt run --select <model>` để chỉ build phần đang sửa, đừng build lại
  cả project.
- Kiểm tra chi phí trước khi chạy thật: `dbt compile` rồi dán SQL vào
  BigQuery console để xem ước tính dung lượng quét.
- 1 TB quét đầu tiên mỗi tháng miễn phí — bộ dữ liệu mẫu này chỉ vài MB.

---

## Quy trình hằng ngày

```powershell
cd C:\Users\tranc\Documents\gcp_project
.\.venv\Scripts\Activate.ps1
. .\scripts\load_env.ps1

dbt build --select <model_dang_sua>   # vòng lặp phát triển
dbt build                             # kiểm tra toàn bộ trước khi commit
```
