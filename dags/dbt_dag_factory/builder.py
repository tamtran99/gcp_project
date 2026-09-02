"""Dựng đối tượng DAG từ DagSpec.

Đồ thị task sinh ra:

    [pre_tasks: generate -> setup_bq -> load_bq]   (tuỳ chọn, nối tiếp)
                    |
                dbt_build          (mặc định: MỘT task duy nhất)
                    |
            publish_artifacts      (đẩy manifest.json ra cho PR build --defer)

Vì sao một `dbt build` chứ không tách `dbt run` -> `dbt test`: `build` xen kẽ
theo từng node — nó test `stg_ecommerce__orders` TRƯỚC khi build `fct_orders` từ
model đó. Tách ra thì dữ liệu bẩn chảy thẳng vào `fct_orders` (incremental
merge), và gỡ ra phải `--full-refresh`. Ai vẫn muốn tách thì đặt
`config.dbt.split_run_test: true`.

`dbt deps` KHÔNG nằm ở đây: nó ghi vào `dbt_packages/` dùng chung của clone, hai
DAG chạy song song sẽ phá nhau. `deps` thuộc về DAG git_sync_main.
"""

from __future__ import annotations

import json
import shlex
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, chain

from . import settings
from .config import ConfigError, DagSpec, DbtSpec

# Thư mục target riêng cho mỗi lần chạy task.
#
# Dùng ts_nodash chứ KHÔNG dùng run_id: run_id của lần chạy theo lịch có dạng
# "scheduled__2026-09-02T02:00:00+00:00", các ký tự ':' và '+' làm tên thư mục rất xấu.
#
# Nếu mọi DAG dùng chung target/ mặc định thì hai lần chạy chồng nhau sẽ ghi đè
# manifest.json / run_results.json / partial_parse.msgpack của nhau.
_RUN_DIR_TEMPLATE = "{repo}/target/airflow/{dag_id}/{{{{ ts_nodash }}}}_{{{{ ti.try_number }}}}"

_BASH_PREAMBLE = "set -Eeuo pipefail\n"


def build_dag(spec: DagSpec) -> DAG:
    """DagSpec -> DAG."""
    params = dict(spec.dag_params)
    params.setdefault("doc_md", _doc_md(spec))

    with DAG(dag_id=spec.dag_id, **params) as dag:
        tasks: list[BashOperator] = [
            _pre_task(pre) for pre in spec.pre_tasks
        ]

        run_dir = _RUN_DIR_TEMPLATE.format(repo=settings.REPO_DIR.as_posix(), dag_id=spec.dag_id)

        if spec.dbt.split_run_test:
            tasks.append(_dbt_task(spec, run_dir, task_id="dbt_run", command="run"))
            # test không retry: chạy lại y hệt thì cũng hỏng y hệt, chỉ tốn tiền quét.
            tasks.append(
                _dbt_task(spec, run_dir, task_id="dbt_test", command="test", retries=0)
            )
        else:
            tasks.append(
                _dbt_task(spec, run_dir, task_id=f"dbt_{spec.dbt.command}",
                          command=spec.dbt.command)
            )

        tasks.append(_publish_artifacts_task(spec))
        chain(*tasks)

    return dag


def build_error_dag(error: ConfigError) -> DAG:
    """DAG giả cho file config hỏng, để lỗi hiện rõ trên UI.

    Không có nó thì một file YAML sai cú pháp chỉ để lại dòng log lặng lẽ trong
    dag-processor, còn trên UI thì DAG đơn giản là biến mất — rất khó nhận ra.
    """
    message = f"[{error.source}] {error.dag_id or '(toàn file)'}: {error.message}"

    with DAG(
        dag_id=error.error_dag_id,
        schedule=None,
        start_date=pendulum.datetime(2026, 1, 1, tz=settings.DEFAULT_TIMEZONE),
        catchup=False,
        tags=["config-error"],
        description="Config trong airflow_config bị lỗi — xem log của task",
        doc_md=f"## Lỗi config\n\n```\n{message}\n```\n",
        max_active_runs=1,
    ) as dag:
        BashOperator(
            task_id="bao_loi_config",
            bash_command=f"echo {shlex.quote(message)} >&2\nexit 1\n",
        )

    return dag


# --- Task -----------------------------------------------------------------


def _pre_task(pre) -> BashOperator:
    repo = settings.REPO_DIR.as_posix()
    script = f"{repo}/{pre.script}"

    # scripts/*.py import kiểu `from _env import ...` nên scripts/ PHẢI nằm trên
    # sys.path, nếu không sẽ ModuleNotFoundError: _env.
    command = (
        f"{_BASH_PREAMBLE}"
        f"cd {shlex.quote(repo)}\n"
        f"export PYTHONPATH={shlex.quote(repo + '/scripts')}"
        '${PYTHONPATH:+:$PYTHONPATH}\n'
        f"exec {shlex.quote(settings.DBT_PY)} {shlex.quote(script)}\n"
    )

    return BashOperator(
        task_id=pre.name,
        bash_command=command,
        cwd=repo,
        append_env=True,
        pool=settings.REPO_POOL,
        pool_slots=1,
        execution_timeout=timedelta(seconds=settings.DEFAULT_DBT_TIMEOUT_SECONDS),
    )


def _dbt_task(
    spec: DagSpec,
    run_dir: str,
    *,
    task_id: str,
    command: str,
    retries: int | None = None,
) -> BashOperator:
    repo = settings.REPO_DIR.as_posix()
    dbt_dir = f"{repo}/target/airflow/{spec.dag_id}"

    body = (
        f"{_BASH_PREAMBLE}"
        f'RUN_DIR="{run_dir}"\n'
        'mkdir -p "$RUN_DIR"\n'
        'export DBT_LOG_PATH="$RUN_DIR/logs"\n'
        f"{_dbt_command(spec.dbt, command, spec.select)}"
        # Symlink ổn định để task publish_artifacts phía sau tìm được manifest
        # mà không phải đoán ts_nodash/try_number của task này.
        f'ln -sfn "$RUN_DIR" {shlex.quote(dbt_dir + "/latest")}\n'
    )

    kwargs = {}
    if retries is not None:
        kwargs["retries"] = retries

    return BashOperator(
        task_id=task_id,
        bash_command=body,
        cwd=repo,
        # append_env=True là BẮT BUỘC. Mặc định False sẽ thay sạch environment,
        # mất PATH/HOME/GCP_PROJECT_ID và mọi env_var() trong profiles.yml sẽ nổ.
        append_env=True,
        env={"DBT_TARGET_NAME": spec.dbt.target},
        pool=settings.REPO_POOL,
        pool_slots=1,
        execution_timeout=timedelta(seconds=settings.DEFAULT_DBT_TIMEOUT_SECONDS),
        **kwargs,
    )


def _publish_artifacts_task(spec: DagSpec) -> BashOperator:
    """Copy manifest.json ra AIRFLOW_HOME để PR build dùng `--defer --state`.

    Nhờ manifest này, PR build chỉ dựng lại model thật sự đổi; model upstream
    không đổi trỏ thẳng vào bảng prod thay vì build lại — cắt phần lớn chi phí quét.
    """
    repo = settings.REPO_DIR.as_posix()
    latest = f"{repo}/target/airflow/{spec.dag_id}/latest"
    state_dir = (settings.DBT_STATE_DIR / spec.dbt.target).as_posix()

    command = (
        f"{_BASH_PREAMBLE}"
        f"SRC={shlex.quote(latest + '/manifest.json')}\n"
        f"DEST_DIR={shlex.quote(state_dir)}\n"
        'if [ ! -f "$SRC" ]; then echo "Không thấy $SRC, bỏ qua"; exit 0; fi\n'
        'mkdir -p "$DEST_DIR"\n'
        # Ghi tạm rồi mv: mv trên cùng filesystem là atomic, nên PR build đang
        # đọc manifest sẽ không bao giờ vớ phải file ghi dở.
        'cp "$SRC" "$DEST_DIR/manifest.json.tmp"\n'
        'mv -f "$DEST_DIR/manifest.json.tmp" "$DEST_DIR/manifest.json"\n'
        'echo "Đã cập nhật $DEST_DIR/manifest.json"\n'
    )

    return BashOperator(
        task_id="publish_artifacts",
        bash_command=command,
        cwd=repo,
        append_env=True,
        retries=0,
    )


# --- Dựng lệnh dbt --------------------------------------------------------


def _dbt_command(dbt: DbtSpec, command: str, select: tuple[str, ...]) -> str:
    repo = settings.REPO_DIR.as_posix()

    # --no-use-colors là global flag, phải đứng TRƯỚC subcommand.
    # Không có nó thì log Airflow đầy escape code ANSI.
    parts = [shlex.quote(settings.DBT_BIN), "--no-use-colors", command]
    parts += ["--target", shlex.quote(dbt.target)]
    parts += ["--project-dir", shlex.quote(repo)]
    # profiles-dir tuyệt đối: .env của repo đặt DBT_PROFILES_DIR=. mà BashOperator
    # chạy ở thư mục tạm, để tương đối là "Could not find profile".
    parts += ["--profiles-dir", shlex.quote(repo)]
    parts += ["--target-path", '"$RUN_DIR"']

    # dbt coi các selector cách nhau bằng dấu cách trong MỘT --select là hợp
    # (union) — đúng ngữ nghĩa mà schema YAML mong muốn.
    if select:
        parts += ["--select", shlex.quote(" ".join(select))]
    if dbt.exclude:
        parts += ["--exclude", shlex.quote(" ".join(dbt.exclude))]
    if dbt.vars:
        parts += ["--vars", shlex.quote(json.dumps(dbt.vars, ensure_ascii=False))]
    if dbt.full_refresh and command in {"run", "build"}:
        parts.append("--full-refresh")
    parts += [shlex.quote(a) for a in dbt.extra_args]

    return " ".join(parts) + "\n"


def _doc_md(spec: DagSpec) -> str:
    lines = [
        f"# {spec.dag_id}",
        "",
        f"Sinh tự động từ `airflow_config/{spec.source}` bởi `dags/dag_builder.py`.",
        "",
        f"- **dbt target**: `{spec.dbt.target}`",
        f"- **Lệnh**: `dbt {spec.dbt.command}`",
        "- **Selector**:",
    ]
    lines += [f"  - `{s}`" for s in spec.select]
    if spec.dbt.exclude:
        lines.append("- **Loại trừ**:")
        lines += [f"  - `{s}`" for s in spec.dbt.exclude]
    if spec.pre_tasks:
        lines.append("- **Chạy trước dbt**:")
        lines += [f"  - `{p.script}`" for p in spec.pre_tasks]
    lines += [
        "",
        "Sửa lịch hay selector thì sửa file YAML rồi push lên `main` — DAG",
        "`git_sync_main` sẽ kéo về và reserialize trong vòng ~5 phút.",
    ]
    return "\n".join(lines)
