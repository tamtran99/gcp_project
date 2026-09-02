"""DAG hạ tầng: kéo code mới nhất từ origin/main về clone mà Airflow đang chạy.

File này là NGUỒN THẬT. Lúc chạy nó được **copy** (không symlink) sang
`$AIRFLOW_HOME/dags_bootstrap/` — một bundle riêng mà git không đụng tới.

Vì sao copy chứ không symlink: nếu DAG sync nằm trong chính thư mục bị
`git reset --hard`, thì một commit hỏng trên main có thể vô hiệu hoá luôn cơ chế
sửa lỗi. Bundle bootstrap tách biệt đảm bảo lúc nào cũng còn đường kéo code về.

Chạy `scripts/airflow_bootstrap.sh --sync-dags` để cập nhật bản copy.
"""

from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, chain, task

REPO_DIR = os.environ.get("GCP_REPO_DIR", str(os.path.expanduser("~/repos/gcp_project")))
REMOTE_URL = os.environ.get(
    "GIT_REMOTE_URL", "https://github.com/tamtran99/gcp_project.git"
)
DBT_BIN = os.environ.get("DBT_BIN", str(os.path.expanduser("~/venvs/dbt/bin/dbt")))
AIRFLOW_BIN = os.environ.get(
    "AIRFLOW_BIN", str(os.path.expanduser("~/venvs/airflow/bin/airflow"))
)
TIMEZONE = os.environ.get("AIRFLOW__CORE__DEFAULT_TIMEZONE", "Asia/Ho_Chi_Minh")

# Pool đóng vai mutex trên clone dùng chung. Task dbt của DAG factory lấy 1 slot;
# hai task dưới đây lấy TOÀN BỘ slot nên không bao giờ chạy đè lên một dbt build
# đang dở. Thiếu khoá này, `git reset --hard` sẽ đổi file .sql giữa lúc dbt đang
# đọc chúng — cho ra một lần chạy nửa cũ nửa mới, gần như không tài nào tái hiện.
REPO_POOL = os.environ.get("GCP_REPO_POOL", "gcp_project_repo")
REPO_POOL_SLOTS = int(os.environ.get("GCP_REPO_POOL_SLOTS", "8"))

BASH_PREAMBLE = "set -Eeuo pipefail\n"


with DAG(
    dag_id="git_sync_main",
    description="Kéo origin/main về clone của Airflow, chạy dbt deps và reserialize",
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz=TIMEZONE),
    catchup=False,
    max_active_runs=1,
    tags=["infra", "git-sync"],
    dagrun_timeout=timedelta(minutes=20),
    default_args={
        "owner": "corpGroup:Data-Platform",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    doc_md=__doc__,
) as dag:

    ensure_clone = BashOperator(
        task_id="ensure_clone",
        bash_command=(
            f"{BASH_PREAMBLE}"
            f'REPO="{REPO_DIR}"\n'
            'if [ ! -d "$REPO/.git" ]; then\n'
            f'  mkdir -p "$(dirname "$REPO")"\n'
            f'  git clone "{REMOTE_URL}" "$REPO"\n'
            '  echo "Đã clone mới $REPO"\n'
            "else\n"
            '  echo "Clone đã có tại $REPO"\n'
            "fi\n"
        ),
        append_env=True,
        pool=REPO_POOL,
        pool_slots=REPO_POOL_SLOTS,
    )

    fetch_and_reset = BashOperator(
        task_id="fetch_and_reset",
        bash_command=(
            f"{BASH_PREAMBLE}"
            f'cd "{REPO_DIR}"\n'
            "git fetch --prune origin main\n"
            'LOCAL="$(git rev-parse HEAD)"\n'
            'REMOTE="$(git rev-parse origin/main)"\n'
            'if [ "$LOCAL" = "$REMOTE" ]; then\n'
            '  echo "NOCHANGE $LOCAL"\n'
            "  exit 0\n"
            "fi\n"
            # Checkout DETACHED chứ không `git reset --hard origin/main`.
            #
            # `reset --hard` dịch nhánh đang được checkout. Nếu ai đó để clone ở
            # nhánh khác (bootstrap có GIT_REF để test PR trước khi merge) thì
            # reset sẽ dời luôn nhánh đó về main — mất code mà không có dấu vết
            # rõ ràng. Detached HEAD không có nhánh nào để hỏng, và nói đúng bản
            # chất: clone này là bản triển khai của origin/main, không phải chỗ
            # làm việc.
            'git checkout --quiet --force --detach "$REMOTE"\n'
            # CẢNH BÁO: KHÔNG BAO GIỜ thêm -x vào lệnh dưới đây.
            # `git clean -fdx` sẽ xoá sạch dbt_packages/, target/, logs/ và
            # data/raw/*.csv — tất cả đều nằm trong .gitignore. Không có -x thì
            # file bị ignore được giữ nguyên, đúng như ta cần.
            "git clean -fd\n"
            'echo "CHANGED $REMOTE"\n'
        ),
        append_env=True,
        # BashOperator đẩy DÒNG STDOUT CUỐI CÙNG vào XCom
        do_xcom_push=True,
        pool=REPO_POOL,
        pool_slots=REPO_POOL_SLOTS,
    )

    @task.short_circuit(task_id="skip_if_unchanged")
    def skip_if_unchanged(status: str | None) -> bool:
        """Chỉ đi tiếp khi origin/main thật sự đổi.

        Không có chốt này thì `dbt deps` chạy 288 lần/ngày cho một repo không hề
        thay đổi — vô ích và làm nhiễu log.
        """
        print(f"Trạng thái từ fetch_and_reset: {status!r}")
        return bool(status and status.startswith("CHANGED"))

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=(
            f"{BASH_PREAMBLE}"
            f'cd "{REPO_DIR}"\n'
            # deps CỐ Ý chỉ ở đây, không nằm trong DAG factory: nó ghi vào
            # dbt_packages/ dùng chung, hai DAG chạy song song sẽ phá nhau.
            f'"{DBT_BIN}" --no-use-colors deps --project-dir "{REPO_DIR}"\n'
        ),
        append_env=True,
        pool=REPO_POOL,
        pool_slots=REPO_POOL_SLOTS,
        execution_timeout=timedelta(minutes=10),
    )

    reserialize = BashOperator(
        task_id="reserialize",
        bash_command=(
            f"{BASH_PREAMBLE}"
            # Ép Airflow đọc lại bundle ngay, thay vì đợi hết refresh_interval.
            f'"{AIRFLOW_BIN}" dags reserialize --bundle-name gcp-project\n'
        ),
        append_env=True,
        execution_timeout=timedelta(minutes=5),
    )

    gc_target_dirs = BashOperator(
        task_id="gc_target_dirs",
        bash_command=(
            f"{BASH_PREAMBLE}"
            f'ROOT="{REPO_DIR}/target/airflow"\n'
            '[ -d "$ROOT" ] || { echo "Chưa có $ROOT"; exit 0; }\n'
            # Mỗi lần chạy task dbt tạo một thư mục target riêng để không ghi đè
            # manifest của nhau. Dọn thư mục cũ hơn 3 ngày, giữ symlink "latest".
            'find "$ROOT" -mindepth 2 -maxdepth 2 -type d -mtime +3 '
            "-exec rm -rf {} + || true\n"
            'echo "Đã dọn target cũ"\n'
        ),
        append_env=True,
        # Chạy độc lập với nhánh short-circuit nên dọn dẹp vẫn diễn ra khi
        # origin/main không đổi.
        trigger_rule="all_done",
    )

    status = fetch_and_reset.output
    chain(ensure_clone, fetch_and_reset, skip_if_unchanged(status), dbt_deps, reserialize)
    chain(fetch_and_reset, gc_target_dirs)
