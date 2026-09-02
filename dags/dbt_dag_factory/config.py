"""Đọc và kiểm tra file airflow_config/*.yml, trả về DagSpec đã validate.

Nguyên tắc thiết kế:

1. **Cô lập lỗi theo từng file.** Một file YAML hỏng chỉ làm hỏng đúng các DAG
   khai báo trong file đó. Các file khác vẫn sinh DAG bình thường.

2. **Allowlist chặt.** Gõ sai tên khoá phải fail rõ ràng chứ không im lặng bị bỏ
   qua — kiểu lỗi im lặng đó rất khó phát hiện vì DAG vẫn xanh mà chạy sai.

3. **Khoá mở rộng chỉ thêm, không phá.** `pre_tasks` và `dbt` đều đọc bằng
   `.get()` nên file YAML theo schema gốc (chỉ có `select` + `dag_params`) parse
   nguyên vẹn, không cần sửa gì.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterator

import pendulum
import yaml

from . import settings
from .schedule import build_schedule, schedule_timezone

# --- Allowlist ------------------------------------------------------------

ALLOWED_TOP = {"is_disabled", "config"}
ALLOWED_CONFIG = {"select", "exclude", "dag_params", "pre_tasks", "dbt"}
ALLOWED_DAG_PARAMS = {
    "tags",
    "schedule",
    "catchup",
    "default_args",
    "start_date",
    "end_date",
    "max_active_runs",
    "max_active_tasks",
    "dagrun_timeout",
    "description",
    "doc_md",
    "params",
}
ALLOWED_DBT = {
    "target",
    "command",
    "exclude",
    "vars",
    "full_refresh",
    "split_run_test",
    "extra_args",
}
ALLOWED_PRE_TASK = {"name", "script"}
ALLOWED_DBT_COMMANDS = {"build", "run", "test", "snapshot", "seed"}

# Airflow 3 đã gỡ SLA. YAML mang từ hệ thống khác sang rất dễ còn sót khoá này.
REMOVED_KEYS = {
    "sla": "SLA đã bị gỡ trong Airflow 3, xem tài liệu 'Deadline Alerts'",
    "sla_miss_callback": "SLA đã bị gỡ trong Airflow 3, xem tài liệu 'Deadline Alerts'",
    "schedule_interval": "Airflow 3 đổi tên thành 'schedule'",
}

DAG_ID_RE = re.compile(r"^[A-Za-z0-9_.-]+$")

# Các khoá trong default_args nhận số giây và cần đổi sang timedelta
_TIMEDELTA_ARGS = {"retry_delay", "execution_timeout", "max_retry_delay"}


# --- Kiểu dữ liệu ---------------------------------------------------------


@dataclass(frozen=True)
class PreTaskSpec:
    """Một script Python chạy trước dbt (nạp CSV lên landing zone)."""

    name: str
    script: str


@dataclass(frozen=True)
class DbtSpec:
    target: str = "prod"
    command: str = "build"
    exclude: tuple[str, ...] = ()
    vars: dict[str, Any] = field(default_factory=dict)
    full_refresh: bool = False
    split_run_test: bool = False
    extra_args: tuple[str, ...] = ()


@dataclass(frozen=True)
class DagSpec:
    dag_id: str
    source: str
    is_disabled: bool
    select: tuple[str, ...]
    dbt: DbtSpec
    pre_tasks: tuple[PreTaskSpec, ...]
    dag_params: dict[str, Any]


@dataclass(frozen=True)
class ConfigError:
    """Một file (hoặc một DAG trong file) không parse được."""

    source: str
    dag_id: str | None
    message: str

    @property
    def error_dag_id(self) -> str:
        stem = Path(self.source).stem or "unknown"
        suffix = self.dag_id or stem
        return f"_config_error__{re.sub(r'[^A-Za-z0-9_.-]', '_', suffix)}"


# --- Điểm vào -------------------------------------------------------------


def load_specs(
    config_dir: Path | None = None,
) -> Iterator[tuple[DagSpec | None, ConfigError | None]]:
    """Duyệt airflow_config/*.yml, yield (spec, None) hoặc (None, error).

    dag_id trùng nhau giữa các file bị coi là lỗi cho lần xuất hiện sau — nếu
    không, DAG nào ghi đè DAG nào sẽ phụ thuộc thứ tự đọc file, rất khó lần ra.
    """
    config_dir = config_dir or settings.CONFIG_DIR
    seen: dict[str, str] = {}

    if not config_dir.is_dir():
        return

    for path in sorted([*config_dir.glob("*.yml"), *config_dir.glob("*.yaml")]):
        source = path.name
        try:
            raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001 — YAML ném nhiều loại lỗi
            yield None, ConfigError(source, None, f"Không đọc được YAML: {exc}")
            continue

        if raw is None:
            continue
        if not isinstance(raw, dict):
            yield None, ConfigError(
                source, None, "File YAML phải là mapping ở mức cao nhất (dag_id: {...})"
            )
            continue

        for dag_id, body in raw.items():
            if not isinstance(dag_id, str) or not DAG_ID_RE.match(dag_id):
                yield None, ConfigError(
                    source, str(dag_id), "dag_id chỉ được chứa chữ, số, '_', '.', '-'"
                )
                continue
            if dag_id in seen:
                yield None, ConfigError(
                    source, dag_id, f"dag_id trùng với khai báo trong {seen[dag_id]}"
                )
                continue
            seen[dag_id] = source

            try:
                yield _parse_dag(dag_id, body, source), None
            except (ValueError, TypeError) as exc:
                yield None, ConfigError(source, dag_id, str(exc))


# --- Parse ----------------------------------------------------------------


def _parse_dag(dag_id: str, body: Any, source: str) -> DagSpec:
    if not isinstance(body, dict):
        raise TypeError("thân của dag_id phải là mapping")
    _reject_unknown(body, ALLOWED_TOP, "khoá mức cao nhất")

    is_disabled = body.get("is_disabled", False)
    if not isinstance(is_disabled, bool):
        raise TypeError(f"is_disabled phải là true/false, nhận {is_disabled!r}")

    config = body.get("config")
    if not isinstance(config, dict):
        raise TypeError("thiếu khoá 'config' hoặc config không phải mapping")
    _reject_unknown(config, ALLOWED_CONFIG, "khoá trong 'config'")

    select = _parse_selectors(config.get("select"), "config.select", required=True)
    dbt = _parse_dbt(config.get("dbt"), config.get("exclude"))
    pre_tasks = _parse_pre_tasks(config.get("pre_tasks"))
    dag_params = _parse_dag_params(config.get("dag_params"), dag_id)

    return DagSpec(
        dag_id=dag_id,
        source=source,
        is_disabled=is_disabled,
        select=select,
        dbt=dbt,
        pre_tasks=pre_tasks,
        dag_params=dag_params,
    )


def _parse_selectors(raw: Any, where: str, *, required: bool) -> tuple[str, ...]:
    if raw is None:
        if required:
            raise ValueError(f"{where} là bắt buộc và không được rỗng")
        return ()
    if isinstance(raw, str):
        raw = [raw]
    if not isinstance(raw, list):
        raise TypeError(f"{where} phải là danh sách chuỗi selector")
    out: list[str] = []
    for item in raw:
        if not isinstance(item, str) or not item.strip():
            raise TypeError(f"{where} chứa phần tử không phải chuỗi: {item!r}")
        out.append(item.strip())
    if required and not out:
        raise ValueError(f"{where} là bắt buộc và không được rỗng")
    return tuple(out)


def _parse_dbt(raw: Any, config_exclude: Any) -> DbtSpec:
    raw = raw or {}
    if not isinstance(raw, dict):
        raise TypeError("config.dbt phải là mapping")
    _reject_unknown(raw, ALLOWED_DBT, "khoá trong 'config.dbt'")

    target = raw.get("target", "prod")
    if not isinstance(target, str) or not target.strip():
        raise TypeError("config.dbt.target phải là chuỗi, ví dụ prod")

    command = raw.get("command", "build")
    if command not in ALLOWED_DBT_COMMANDS:
        raise ValueError(
            f"config.dbt.command không hợp lệ: {command!r}. "
            f"Chấp nhận: {sorted(ALLOWED_DBT_COMMANDS)}"
        )

    # exclude có thể khai ở config.exclude (schema gốc) hoặc config.dbt.exclude
    exclude = _parse_selectors(config_exclude, "config.exclude", required=False)
    exclude += _parse_selectors(raw.get("exclude"), "config.dbt.exclude", required=False)

    dbt_vars = raw.get("vars") or {}
    if not isinstance(dbt_vars, dict):
        raise TypeError("config.dbt.vars phải là mapping")

    for flag in ("full_refresh", "split_run_test"):
        value = raw.get(flag, False)
        if not isinstance(value, bool):
            raise TypeError(f"config.dbt.{flag} phải là true/false")

    extra_args = raw.get("extra_args") or []
    if not isinstance(extra_args, list) or any(
        not isinstance(a, str) for a in extra_args
    ):
        raise TypeError("config.dbt.extra_args phải là danh sách chuỗi")

    return DbtSpec(
        target=target.strip(),
        command=command,
        exclude=exclude,
        vars=dbt_vars,
        full_refresh=bool(raw.get("full_refresh", False)),
        split_run_test=bool(raw.get("split_run_test", False)),
        extra_args=tuple(extra_args),
    )


def _parse_pre_tasks(raw: Any) -> tuple[PreTaskSpec, ...]:
    if raw is None:
        return ()
    if not isinstance(raw, list):
        raise TypeError("config.pre_tasks phải là danh sách")

    out: list[PreTaskSpec] = []
    for item in raw:
        if not isinstance(item, dict):
            raise TypeError("mỗi phần tử của config.pre_tasks phải là mapping")
        _reject_unknown(item, ALLOWED_PRE_TASK, "khoá trong 'pre_tasks'")
        script = item.get("script")
        if not isinstance(script, str) or not script.strip():
            raise TypeError("pre_tasks[].script là bắt buộc, đường dẫn tương đối repo")
        script = script.strip()
        if script.startswith("/") or ".." in Path(script).parts:
            raise ValueError(
                f"pre_tasks[].script phải là đường dẫn tương đối gốc repo: {script!r}"
            )
        name = item.get("name") or Path(script).stem
        if not isinstance(name, str) or not DAG_ID_RE.match(name):
            raise ValueError(f"pre_tasks[].name không hợp lệ: {name!r}")
        out.append(PreTaskSpec(name=name, script=script))
    return tuple(out)


def _parse_dag_params(raw: Any, dag_id: str) -> dict[str, Any]:
    raw = raw or {}
    if not isinstance(raw, dict):
        raise TypeError("config.dag_params phải là mapping")
    _reject_removed(raw, "config.dag_params")
    _reject_unknown(raw, ALLOWED_DAG_PARAMS, "khoá trong 'config.dag_params'")

    params = dict(raw)

    # tags: bắt buộc toàn chuỗi khác rỗng.
    tags = params.get("tags")
    if tags is not None:
        if not isinstance(tags, list) or any(not isinstance(t, str) for t in tags):
            raise TypeError("config.dag_params.tags phải là danh sách chuỗi")
        # `tags: [...]` là placeholder hay gặp khi copy YAML từ nơi khác sang.
        # PyYAML trả về chuỗi '...' chứ không phải Ellipsis nên kiểm tra kiểu
        # không bắt được — phải chặn thẳng giá trị.
        bad = [t for t in tags if not t.strip() or t.strip() == "..."]
        if bad:
            raise ValueError(
                f"config.dag_params.tags còn placeholder chưa điền: {bad}. "
                "Thay '...' bằng tag thật."
            )

    catchup = params.get("catchup", False)
    if not isinstance(catchup, bool):
        raise TypeError("config.dag_params.catchup phải là true/false")
    params["catchup"] = catchup

    schedule_raw = params.get("schedule")
    params["schedule"] = build_schedule(schedule_raw)

    # Canh start_date theo timezone của schedule. Nếu để lệch, timetable chạy
    # theo giờ VN nhưng cột "Next Run" trên UI render theo dag.timezone (UTC)
    # -> nhìn như sai giờ dù thực tế chạy đúng.
    tz = schedule_timezone(schedule_raw) or settings.DEFAULT_TIMEZONE
    params["start_date"] = _to_datetime(
        params.get("start_date", settings.DEFAULT_START_DATE), tz, "start_date"
    )
    if params.get("end_date") is not None:
        params["end_date"] = _to_datetime(params["end_date"], tz, "end_date")

    if params.get("dagrun_timeout") is not None:
        params["dagrun_timeout"] = _to_timedelta(
            params["dagrun_timeout"], "dagrun_timeout"
        )

    params["default_args"] = _parse_default_args(params.get("default_args"))
    params.setdefault("max_active_runs", 1)
    params.setdefault("description", f"Sinh tự động từ airflow_config cho {dag_id}")

    return params


def _parse_default_args(raw: Any) -> dict[str, Any]:
    raw = raw or {}
    if not isinstance(raw, dict):
        raise TypeError("config.dag_params.default_args phải là mapping")
    _reject_removed(raw, "config.dag_params.default_args")

    args = dict(raw)
    for key in _TIMEDELTA_ARGS:
        if args.get(key) is not None:
            args[key] = _to_timedelta(args[key], f"default_args.{key}")
    args.setdefault("owner", "airflow")
    args.setdefault("retries", 1)
    args.setdefault("retry_delay", timedelta(minutes=5))
    args.setdefault("depends_on_past", False)
    return args


# --- Tiện ích -------------------------------------------------------------


def _reject_unknown(mapping: dict, allowed: set[str], where: str) -> None:
    unknown = set(mapping) - allowed
    if unknown:
        raise ValueError(
            f"{where} không hợp lệ: {sorted(unknown)}. Chấp nhận: {sorted(allowed)}"
        )


def _reject_removed(mapping: dict, where: str) -> None:
    for key, reason in REMOVED_KEYS.items():
        if key in mapping:
            raise ValueError(f"{where}.{key} không còn dùng được: {reason}")


def _to_datetime(value: Any, tz: str, where: str) -> pendulum.DateTime:
    """YAML có thể trả về str, date hoặc datetime tuỳ cách viết. Chuẩn hoá hết."""
    if isinstance(value, pendulum.DateTime):
        return value if value.tzinfo else value.in_timezone(tz)
    if isinstance(value, datetime):
        return pendulum.instance(value, tz=tz) if value.tzinfo is None else pendulum.instance(value)
    if isinstance(value, date):
        return pendulum.datetime(value.year, value.month, value.day, tz=tz)
    if isinstance(value, str):
        try:
            parsed = pendulum.parse(value.strip(), tz=tz)
        except Exception as exc:  # noqa: BLE001
            raise ValueError(f"{where} không parse được: {value!r} ({exc})") from exc
        if not isinstance(parsed, pendulum.DateTime):
            raise ValueError(f"{where} phải là ngày/giờ, nhận {value!r}")
        return parsed
    raise TypeError(f"{where} phải là chuỗi ngày hoặc ngày YAML, nhận {value!r}")


def _to_timedelta(value: Any, where: str) -> timedelta:
    """Nhận số giây (int/float) hoặc timedelta có sẵn."""
    if isinstance(value, timedelta):
        return value
    if isinstance(value, bool):  # bool là subclass của int, chặn trước
        raise TypeError(f"{where} phải là số giây, nhận {value!r}")
    if isinstance(value, (int, float)):
        return timedelta(seconds=float(value))
    raise TypeError(f"{where} phải là số giây (int), nhận {value!r}")
