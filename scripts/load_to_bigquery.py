"""Nạp các file CSV trong data/raw/ lên BigQuery (landing zone).

Đặc điểm:
  * Khai báo schema TƯỜNG MINH thay vì để BigQuery tự đoán — tự đoán rất
    hay sai kiểu khi cột có giá trị rỗng.
  * WRITE_TRUNCATE: mỗi lần chạy ghi đè toàn bộ bảng, nên chạy lại bao
    nhiêu lần cũng cho kết quả giống nhau (idempotent).
  * Bảng orders được partition theo order_date và cluster theo
    customer_id -> query lọc theo ngày sẽ quét ít dữ liệu, rẻ hơn.

Chạy:  python scripts/load_to_bigquery.py
"""

from __future__ import annotations

import sys

from google.api_core import exceptions as gcp_exceptions
from google.cloud import bigquery

from _env import LOCATION, PROJECT_ID, RAW_DATASET, RAW_DIR

SF = bigquery.SchemaField

# Mỗi entry: tên bảng -> (tên file, schema, cột partition, cột cluster)
TABLES: dict[str, tuple] = {
    "customers": (
        "customers.csv",
        [
            SF("customer_id", "STRING", mode="REQUIRED"),
            SF("first_name", "STRING"),
            SF("last_name", "STRING"),
            SF("email", "STRING"),
            SF("country", "STRING"),
            SF("city", "STRING"),
            SF("signup_date", "DATE"),
            SF("is_active", "BOOL"),
        ],
        None,
        ["country"],
    ),
    "products": (
        "products.csv",
        [
            SF("product_id", "STRING", mode="REQUIRED"),
            SF("product_name", "STRING"),
            SF("category", "STRING"),
            SF("brand", "STRING"),
            SF("unit_price", "NUMERIC"),
            SF("cost_price", "NUMERIC"),
            SF("is_discontinued", "BOOL"),
        ],
        None,
        ["category"],
    ),
    "orders": (
        "orders.csv",
        [
            SF("order_id", "STRING", mode="REQUIRED"),
            SF("customer_id", "STRING", mode="REQUIRED"),
            SF("order_date", "DATE", mode="REQUIRED"),
            SF("order_status", "STRING"),
            SF("payment_method", "STRING"),
        ],
        "order_date",
        ["customer_id"],
    ),
    "order_items": (
        "order_items.csv",
        [
            SF("order_item_id", "STRING", mode="REQUIRED"),
            SF("order_id", "STRING", mode="REQUIRED"),
            SF("product_id", "STRING", mode="REQUIRED"),
            SF("quantity", "INT64"),
            SF("unit_price", "NUMERIC"),
            SF("discount_pct", "INT64"),
        ],
        None,
        ["order_id", "product_id"],
    ),
}


def load_table(
    client: bigquery.Client,
    table_name: str,
    filename: str,
    schema: list,
    partition_field: str | None,
    cluster_fields: list[str] | None,
) -> None:
    csv_path = RAW_DIR / filename
    if not csv_path.exists():
        sys.exit(
            f"[X] Khong tim thay {csv_path}\n"
            f"    Chay truoc: python scripts/generate_sample_data.py"
        )

    table_id = f"{PROJECT_ID}.{RAW_DATASET}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        schema=schema,
        skip_leading_rows=1,          # bỏ dòng header
        write_disposition="WRITE_TRUNCATE",
        # Cho phép ô rỗng -> NULL thay vì làm hỏng cả job
        allow_quoted_newlines=True,
        max_bad_records=0,
    )
    if partition_field:
        job_config.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field=partition_field
        )
    if cluster_fields:
        job_config.clustering_fields = cluster_fields

    with csv_path.open("rb") as source_file:
        job = client.load_table_from_file(
            source_file, table_id, job_config=job_config, location=LOCATION
        )

    try:
        job.result()  # chờ job chạy xong
    except gcp_exceptions.BadRequest as exc:
        errors = "\n      ".join(str(e) for e in (job.errors or []))
        sys.exit(f"[X] Load {table_name} that bai: {exc}\n      {errors}")

    table = client.get_table(table_id)
    size_mb = (table.num_bytes or 0) / 1024 / 1024
    print(
        f"  [OK] {table_name:<15} {table.num_rows:>7,} dong  "
        f"{size_mb:>6.2f} MB"
    )


def main() -> None:
    print(f"Nap du lieu -> {PROJECT_ID}.{RAW_DATASET} ({LOCATION})\n")

    client = bigquery.Client(project=PROJECT_ID, location=LOCATION)

    # Dataset phải tồn tại trước, nếu chưa thì nhắc chạy setup
    try:
        client.get_dataset(f"{PROJECT_ID}.{RAW_DATASET}")
    except gcp_exceptions.NotFound:
        sys.exit(
            f"[X] Dataset {RAW_DATASET} chua ton tai.\n"
            f"    Chay truoc: python scripts/setup_bigquery.py"
        )

    for table_name, (filename, schema, part, cluster) in TABLES.items():
        load_table(client, table_name, filename, schema, part, cluster)

    print("\nXong. Buoc tiep theo:  dbt deps  ->  dbt build")


if __name__ == "__main__":
    main()
