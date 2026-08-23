"""Đọc cấu hình chung cho các script Python.

Ưu tiên biến môi trường đã có sẵn trong session (do load_env.ps1 nạp),
nếu chưa có thì đọc từ file .env ở gốc repo.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]

# override=False: không ghi đè biến đã set sẵn trong session
load_dotenv(REPO_ROOT / ".env", override=False)


def require(name: str) -> str:
    """Lấy biến bắt buộc, báo lỗi rõ ràng thay vì KeyError khó hiểu."""
    value = os.getenv(name, "").strip()
    if not value:
        sys.exit(
            f"[X] Thieu bien moi truong {name}.\n"
            f"    Kiem tra file .env tai {REPO_ROOT / '.env'}\n"
            f"    (tao bang: Copy-Item .env.example .env)"
        )
    return value


PROJECT_ID = require("GCP_PROJECT_ID")
LOCATION = os.getenv("BQ_LOCATION", "asia-southeast1").strip()
RAW_DATASET = os.getenv("BQ_RAW_DATASET", "raw_ecommerce").strip()
DBT_DATASET = os.getenv("BQ_DBT_DATASET", "dbt_dev").strip()

RAW_DIR = REPO_ROOT / "data" / "raw"
