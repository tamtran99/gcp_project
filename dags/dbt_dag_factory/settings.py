"""Hằng số và đường dẫn runtime, đọc từ biến môi trường.

Mọi giá trị đều có mặc định suy ra được, nên `python dags/dag_builder.py` chạy
smoke test ngoài Airflow vẫn import được module này mà không nổ.

Nguồn thật của các biến này là ~/airflow/airflow-env.sh (xem scripts/airflow_bootstrap.sh).
"""

from __future__ import annotations

import os
from pathlib import Path

# --- Đường dẫn ------------------------------------------------------------

# Thư mục repo. Mặc định suy từ vị trí file: dags/dbt_dag_factory/settings.py
# -> parents[2] = gốc repo. Biến GCP_REPO_DIR ghi đè khi Airflow chạy trên clone.
REPO_DIR = Path(
    os.environ.get("GCP_REPO_DIR") or Path(__file__).resolve().parents[2]
).resolve()

# Thư mục chứa file .yml khai báo DAG
CONFIG_DIR = REPO_DIR / "airflow_config"

# Nơi cất manifest.json của lần build prod gần nhất, dùng cho `--defer` khi build PR
AIRFLOW_HOME = Path(
    os.environ.get("AIRFLOW_HOME") or (Path.home() / "airflow")
).resolve()
DBT_STATE_DIR = AIRFLOW_HOME / "dbt_state"

# --- Binary ---------------------------------------------------------------

# dbt và Python nằm trong venv RIÊNG, không phải venv của Airflow.
# Trộn chung sẽ xung đột jinja2/protobuf/click giữa dbt-core 1.12 và airflow-core.
DBT_BIN = os.environ.get("DBT_BIN") or str(Path.home() / "venvs/dbt/bin/dbt")
DBT_PY = os.environ.get("DBT_PY") or str(Path.home() / "venvs/dbt/bin/python")

# --- Điều phối ------------------------------------------------------------

# Pool đóng vai mutex trên clone dùng chung: task dbt lấy 1 slot, còn
# git reset/dbt deps trong git_sync_main lấy TOÀN BỘ slot. Nhờ vậy không bao giờ
# có chuyện `git reset --hard` đổi file .sql giữa lúc dbt đang build.
REPO_POOL = os.environ.get("GCP_REPO_POOL") or "gcp_project_repo"
REPO_POOL_SLOTS = int(os.environ.get("GCP_REPO_POOL_SLOTS") or "8")

# Mặc định cho DAG không khai báo start_date trong YAML
DEFAULT_START_DATE = os.environ.get("DAG_DEFAULT_START_DATE") or "2026-01-01"
DEFAULT_TIMEZONE = os.environ.get("AIRFLOW__CORE__DEFAULT_TIMEZONE") or "Asia/Ho_Chi_Minh"

# Timeout mặc định cho 1 task dbt (giây)
DEFAULT_DBT_TIMEOUT_SECONDS = int(os.environ.get("DBT_TASK_TIMEOUT") or "2700")
