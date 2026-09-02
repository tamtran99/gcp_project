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
import urllib.error
import urllib.parse
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

# URL gốc của Airflow UI, dùng làm target_url cho status trên GitHub. Người bấm
# vào link đang ngồi cùng máy nên localhost là đủ; đổi khi Airflow chạy nơi khác.
AIRFLOW_BASE_URL = os.environ.get("AIRFLOW_BASE_URL", "http://localhost:8080")

# GitHub gom status theo `context`. Đổi chuỗi này = tạo ra một check hoàn toàn
# mới, còn check cũ nằm lại vĩnh viễn ở trạng thái cuối cùng của nó.
STATUS_CONTEXT = "airflow/dbt-pr-build"


def _github(method: str, path: str, token: str | None, payload=None):
    """Gọi GitHub API, trả về JSON đã parse (hoặc None khi body rỗng)."""
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "airflow-git-pr-builds",
    }
    if data is not None:
        headers["Content-Type"] = "application/json"
    if token:
        headers["Authorization"] = f"Bearer {token}"

    request = urllib.request.Request(
        f"https://api.github.com{path}", data=data, headers=headers, method=method
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        body = response.read()
    return json.loads(body) if body else None


def _post_status(token, sha, state, description, target_url=None):
    """Gắn trạng thái build lên đúng commit của PR.

    Dùng Statuses API chứ không phải Checks API: Checks API chỉ cho GitHub App
    ghi, còn personal access token thì không dùng được.
    """
    payload = {
        "state": state,  # pending | success | failure | error
        "context": STATUS_CONTEXT,
        # GitHub cắt description ở 140 ký tự
        "description": description[:140],
    }
    if target_url:
        payload["target_url"] = target_url
    return _github("POST", f"/repos/{GITHUB_REPO}/statuses/{sha}", token, payload)


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

    @task(task_id="bao_dang_build")
    def bao_dang_build(prs: list[dict[str, str]]) -> int:
        """Gắn 'pending' lên commit của từng PR sắp được build.

        Đặt trước khi build để trong lúc dbt chạy (có thể tới 45 phút) thì trang
        PR đã hiện là đang có việc, thay vì trông như chưa ai đụng tới.
        """
        token = Variable.get(GITHUB_TOKEN_VAR, default=None)
        if not token:
            print("Chưa đặt Variable 'github_token' — bỏ qua báo trạng thái.")
            print("Đặt bằng: airflow variables set github_token '<token>'")
            return 0

        count = 0
        for pull in prs:
            try:
                _post_status(
                    token,
                    pull["PR_HEAD_SHA"],
                    "pending",
                    f"dbt build vào {pull['BQ_CI_DATASET']}_*",
                )
                count += 1
            except urllib.error.HTTPError as exc:
                # Không đẩy được status thì vẫn phải build. Thiếu status là bất
                # tiện; chặn cả pipeline vì nó mới là hỏng.
                print(f"PR #{pull['PR_NUMBER']}: không đặt được pending ({exc.code})")
        print(f"Đã đặt pending cho {count}/{len(prs)} PR")
        return count

    @task(task_id="bao_ket_qua", trigger_rule="all_done")
    def bao_ket_qua(prs: list[dict[str, str]], **context) -> dict[str, str]:
        """Đẩy kết quả cuối lên GitHub, kèm link tới log Airflow.

        PR nào không đẩy được dòng `BUILT` vào XCom thì coi là hỏng — task map
        của nó fail nên không push gì cả.
        """
        token = Variable.get(GITHUB_TOKEN_VAR, default=None)
        if not token:
            print("Chưa đặt Variable 'github_token' — bỏ qua báo trạng thái.")
            return {}

        results = context["ti"].xcom_pull(task_ids="build_pr") or []
        if isinstance(results, str):
            results = [results]
        thanh_cong = {
            line.split()[1]
            for line in results
            if line and line.startswith("BUILT ")
        }

        run_id = urllib.parse.quote(context["dag_run"].run_id, safe="")
        log_url = f"{AIRFLOW_BASE_URL}/dags/git_pr_builds/runs/{run_id}"

        ket_qua: dict[str, str] = {}
        for pull in prs:
            so = pull["PR_NUMBER"]
            ok = so in thanh_cong
            state = "success" if ok else "failure"
            mo_ta = (
                f"dbt build xong → {pull['BQ_CI_DATASET']}_marts"
                if ok
                else "dbt build hỏng — mở log Airflow để xem"
            )
            try:
                _post_status(token, pull["PR_HEAD_SHA"], state, mo_ta, log_url)
                ket_qua[so] = state
                print(f"PR #{so}: {state}")
            except urllib.error.HTTPError as exc:
                print(f"PR #{so}: không đẩy được status ({exc.code})")
        return ket_qua

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

    chain(bao_dang_build(pr_envs), build_pr)
    chain(build_pr, record_built_shas(), cleanup, ket_thuc)
    chain(build_pr, bao_ket_qua(pr_envs))
    chain(build_pr, ket_thuc)
