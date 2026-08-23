"""Kiểm tra kết nối GCP và tạo sẵn các dataset cần dùng.

Chạy script này TRƯỚC khi load dữ liệu. Nó sẽ:
  1. Xác thực bằng Application Default Credentials (gcloud auth ...)
  2. Tạo dataset raw (landing zone) nếu chưa có
  3. Tạo dataset đích của dbt nếu chưa có

Chạy:  python scripts/setup_bigquery.py
"""

from __future__ import annotations

import sys

from google.api_core import exceptions as gcp_exceptions
from google.cloud import bigquery

from _env import DBT_DATASET, LOCATION, PROJECT_ID, RAW_DATASET


def ensure_dataset(client: bigquery.Client, dataset_id: str, desc: str) -> None:
    """Tạo dataset nếu chưa tồn tại. An toàn khi chạy lại nhiều lần."""
    ref = bigquery.Dataset(f"{PROJECT_ID}.{dataset_id}")
    ref.location = LOCATION
    ref.description = desc

    try:
        existing = client.get_dataset(ref)
        # Dataset đã có nhưng khác region -> mọi query sau sẽ lỗi khó hiểu,
        # nên cảnh báo ngay tại đây.
        if existing.location.upper() != LOCATION.upper():
            print(
                f"  [!] {dataset_id} dang o location '{existing.location}' "
                f"nhung .env khai bao '{LOCATION}'. "
                f"Sua BQ_LOCATION cho khop, hoac xoa dataset va tao lai."
            )
        else:
            print(f"  [=] {dataset_id:<20} da ton tai ({existing.location})")
    except gcp_exceptions.NotFound:
        client.create_dataset(ref)
        print(f"  [+] {dataset_id:<20} da tao moi ({LOCATION})")


def main() -> None:
    print(f"Project : {PROJECT_ID}")
    print(f"Location: {LOCATION}\n")

    try:
        client = bigquery.Client(project=PROJECT_ID, location=LOCATION)
        # Gọi một API rẻ tiền để xác nhận credential + quyền thực sự dùng được
        list(client.list_datasets(max_results=1))
    except gcp_exceptions.Forbidden as exc:
        sys.exit(
            f"[X] Khong du quyen tren project {PROJECT_ID}.\n"
            f"    Can role 'BigQuery Data Editor' + 'BigQuery Job User'.\n"
            f"    Chi tiet: {exc}"
        )
    except Exception as exc:  # noqa: BLE001 - muốn bắt mọi lỗi auth
        sys.exit(
            f"[X] Khong ket noi duoc BigQuery: {exc}\n\n"
            f"    Thu chay lai:\n"
            f"      gcloud auth application-default login\n"
            f"      gcloud config set project {PROJECT_ID}"
        )

    print("[OK] Ket noi BigQuery thanh cong.\n")
    print("Kiem tra dataset:")
    ensure_dataset(client, RAW_DATASET, "Du lieu tho nap tu file CSV")
    ensure_dataset(client, DBT_DATASET, "Dataset dich cho dbt (moi truong dev)")

    print("\nXong. Buoc tiep theo: python scripts/load_to_bigquery.py")


if __name__ == "__main__":
    main()
