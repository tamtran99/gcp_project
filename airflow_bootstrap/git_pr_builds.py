"""DAG hạ tầng: build thử mọi Pull Request đang mở, vào dataset cô lập.

Cơ chế là **poll** chứ không phải webhook, vì Airflow chạy local trong WSL nên
GitHub không gọi vào được. Mỗi 15 phút DAG hỏi GitHub API danh sách PR mở, và
chỉ build những PR có commit mới kể từ lần build trước.

An toàn với production: build PR dùng `--target ci`, KHÔNG phải `prod`.
`macros/generate_schema_name.sql` chỉ trả tên schema thô khi
`target.name == 'prod'`; với target `ci` nó thêm tiền tố dataset, nên PR #42 ghi
vào `pr_42_staging` / `pr_42_intermediate` / `pr_42_marts`. Nếu dùng `prod` ở đây
thì mỗi PR sẽ đè thẳng lên bảng production, 15 phút một lần.

File này là NGUỒN THẬT; lúc chạy nó được copy sang `$AIRFLOW_HOME/dags_bootstrap/`.
"""

from __future__ import annotations

import json
import os
import urllib.request
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, Variable, chain, task

REPO_DIR = os.environ.get("GCP_REPO_DIR", os.path.expanduser("~/repos/gcp_project"))
WORKTREE_ROOT = os.environ.get(
    "PR_WORKTREE_ROOT", os.path.expanduser("~/repos/pr_worktrees")
)
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", os.path.expanduser("~/airflow"))
DBT_BIN = os.environ.get("DBT_BIN", os.path.expanduser("~/venvs/dbt/bin/dbt"))
DBT_PY = os.environ.get("DBT_PY", os.path.expanduser("~/venvs/dbt/bin/python"))
GITHUB_REPO = os.environ.get("GITHUB_REPO", "tamtran99/gcp_project")
TIMEZONE = os.environ.get("AIRFLOW__CORE__DEFAULT_TIMEZONE", "Asia/Ho_Chi_Minh")

REPO_POOL = os.environ.get("GCP_REPO_POOL", "gcp_project_repo")

# Tên Variable lưu SHA đã build của từng PR, để không build lại commit cũ
BUILT_SHAS_VAR = "pr_last_built_sha"
GITHUB_TOKEN_VAR = "github_token"

BASH_PREAMBLE = "set -Eeuo pipefail\n"

BUILD_PR_COMMAND = f"""{BASH_PREAMBLE}
REPO="{REPO_DIR}"
WT="{WORKTREE_ROOT}/pr-$PR_NUMBER"
STATE_DIR="{AIRFLOW_HOME}/dbt_state/prod"

mkdir -p "{WORKTREE_ROOT}"
cd "$REPO"

# Fetch vào object store của clone chung — đây là lý do task này cần giữ một
# slot của pool, dù bản checkout (worktree) thì hoàn toàn tách biệt.
git fetch origin "pull/$PR_NUMBER/head:refs/heads/pr-$PR_NUMBER" --force
git worktree remove --force "$WT" >/dev/null 2>&1 || true
git worktree add --force "$WT" "pr-$PR_NUMBER"
trap 'git -C "$REPO" worktree remove --force "$WT" >/dev/null 2>&1 || true' EXIT

"{DBT_BIN}" --no-use-colors deps --project-dir "$WT"

if [ -f "$STATE_DIR/manifest.json" ]; then
  # --defer: model upstream không đổi trỏ thẳng vào bảng prod thay vì dựng lại.
  # Đây là thứ giữ chi phí quét BigQuery của mỗi PR ở mức chấp nhận được.
  echo "Có manifest prod -> build phần thay đổi (state:modified+)"
  SEL=(--select 'state:modified+' --defer --state "$STATE_DIR")
else
  echo "Chưa có manifest prod -> build toàn bộ models"
  SEL=(--select 'path:models')
fi

"{DBT_BIN}" --no-use-colors build \\
  --target ci \\
  --project-dir "$WT" \\
  --profiles-dir "$WT" \\
  --target-path "$WT/target" \\
  "${{SEL[@]}}"

echo "BUILT $PR_NUMBER $PR_HEAD_SHA"
"""


with DAG(
    dag_id="git_pr_builds",
    description="Poll PR đang mở trên GitHub, build vào dataset pr_<số> riêng",
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz=TIMEZONE),
    catchup=False,
    max_active_runs=1,
    tags=["infra", "git-sync", "ci"],
    dagrun_timeout=timedelta(hours=1),
    default_args={
        "owner": "corpGroup:Data-Platform",
        "retries": 0,
        "retry_delay": timedelta(minutes=2),
    },
    doc_md=__doc__,
) as dag:

    @task(task_id="list_open_prs")
    def list_open_prs() -> list[dict[str, str]]:
        """Trả về env cho từng PR CẦN build (commit mới so với lần trước).

        Chốt chặn chi phí quan trọng nhất của cả thiết kế: không có bộ so SHA
        này thì 5 PR mở = 480 lần dbt build lên BigQuery mỗi ngày, toàn bộ đều
        build lại đúng thứ đã build xong.
        """
        # Variable đọc TRONG task, không đọc ở module level: đọc ở module level
        # là mỗi vòng parse (30 giây) lại một truy vấn metadata DB, mãi mãi.
        token = Variable.get(GITHUB_TOKEN_VAR, default=None)
        built: dict[str, str] = Variable.get(
            BUILT_SHAS_VAR, default={}, deserialize_json=True
        )

        url = f"https://api.github.com/repos/{GITHUB_REPO}/pulls?state=open&per_page=100"
        headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "airflow-git-pr-builds",
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"

        request = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(request, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))

        to_build: list[dict[str, str]] = []
        for pull in payload:
            number = str(pull["number"])
            head_sha = pull["head"]["sha"]
            if built.get(number) == head_sha:
                print(f"PR #{number}: SHA không đổi ({head_sha[:8]}), bỏ qua")
                continue
            print(f"PR #{number} [{pull['head']['ref']}]: cần build {head_sha[:8]}")
            to_build.append(
                {
                    "PR_NUMBER": number,
                    "PR_HEAD_SHA": head_sha,
                    # Macro generate_schema_name sẽ thêm tiền tố này vào từng
                    # schema -> pr_42_staging, pr_42_marts...
                    "BQ_CI_DATASET": f"pr_{number}",
                }
            )

        print(f"Tổng cộng {len(to_build)}/{len(payload)} PR cần build")
        return to_build

    pr_envs = list_open_prs()

    build_pr = BashOperator.partial(
        task_id="build_pr",
        bash_command=BUILD_PR_COMMAND,
        cwd=REPO_DIR,
        append_env=True,
        pool=REPO_POOL,
        pool_slots=1,
        # Chạy tối đa 2 PR song song để không đốt quota BigQuery cùng lúc
        max_active_tis_per_dag=2,
        do_xcom_push=True,
        execution_timeout=timedelta(minutes=45),
    ).expand(env=pr_envs)

    @task(task_id="record_built_shas", trigger_rule="all_done")
    def record_built_shas(**context) -> dict[str, str]:
        """Ghi nhận SHA của các PR build THÀNH CÔNG.

        PR build hỏng cố ý không được ghi nhận, để vòng sau thử lại.

        Cố ý kéo XCom bằng `ti.xcom_pull` chứ không nhận qua tham số: task này
        chạy với trigger_rule all_done, nên trong số các task map phía trên sẽ
        có cái hỏng và không đẩy XCom nào cả. Nhận qua tham số thì việc resolve
        XComArg đó có thể nổ trước khi thân hàm kịp chạy.
        """
        results = context["ti"].xcom_pull(task_ids="build_pr") or []
        if isinstance(results, str):
            results = [results]
        built: dict[str, str] = Variable.get(
            BUILT_SHAS_VAR, default={}, deserialize_json=True
        )
        for line in results:
            if not line or not line.startswith("BUILT "):
                continue
            _, number, sha = line.split()
            built[number] = sha
            print(f"Ghi nhận PR #{number} = {sha[:8]}")
        Variable.set(BUILT_SHAS_VAR, built, serialize_json=True)
        return built

    cleanup = BashOperator(
        task_id="cleanup_closed_prs",
        bash_command=(
            f"{BASH_PREAMBLE}"
            f'cd "{REPO_DIR}"\n'
            "git worktree prune\n"
            # scripts/*.py import `from _env import ...` nên scripts/ phải có
            # trên sys.path.
            f'export PYTHONPATH="{REPO_DIR}/scripts"${{PYTHONPATH:+:$PYTHONPATH}}\n'
            f'"{DBT_PY}" "{REPO_DIR}/scripts/cleanup_pr_datasets.py" '
            f'--github-repo "{GITHUB_REPO}"\n'
        ),
        append_env=True,
        # Dọn dẹp phải chạy kể cả khi có PR build hỏng, nếu không dataset pr_*
        # của PR đã đóng sẽ tồn tại mãi và tính tiền lưu trữ.
        trigger_rule="all_done",
        execution_timeout=timedelta(minutes=15),
    )

    # cleanup PHẢI chạy kể cả khi build hỏng (all_done), nhưng nếu nó là leaf
    # task thì Airflow lấy trạng thái leaf làm trạng thái DAG run — một PR build
    # hỏng sẽ hiện thành run màu xanh. Task rỗng này giữ lại tín hiệu đó.
    ket_thuc = EmptyOperator(task_id="ket_thuc", trigger_rule="all_success")

    chain(build_pr, record_built_shas(), cleanup, ket_thuc)
    chain(build_pr, ket_thuc)
