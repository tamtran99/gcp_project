"""Chuyển khoá `schedule` đa hình trong YAML thành tham số `schedule` của DAG.

Schema hỗ trợ hai dạng:

    schedule: "0 * * * *"                     # chuỗi cron trần (hoặc preset @daily)

    schedule:                                 # dạng object, chốt luôn timezone
      cron: "15 * * * *"
      timezone: Asia/Ho_Chi_Minh

Vì sao phải có `CronTriggerTimetable` cho dạng object: trong Airflow 3,
`DAG.timezone` là attrs field `init=False` — KHÔNG truyền `timezone=` vào
`DAG(...)` được. Timezone của DAG suy ra từ `start_date.tzinfo`, fallback
`[core] default_timezone`. Nên cách duy nhất để ghim timezone cho lịch chạy là
dựng thẳng timetable.

Hai dạng cho ra CÙNG một class: chuỗi cron trần được Airflow tự bọc thành
`CronTriggerTimetable(cron, timezone=dag.timezone)` (do `[scheduler]
create_cron_data_intervals` mặc định False trong Airflow 3). Dạng object chỉ
khác ở chỗ nói rõ timezone thay vì đi mượn của DAG.
"""

from __future__ import annotations

from typing import Any

import pendulum
from airflow.sdk import CronTriggerTimetable

ALLOWED_SCHEDULE_KEYS = {"cron", "timezone"}


def build_schedule(raw: Any) -> Any:
    """YAML `schedule` -> giá trị truyền vào tham số `schedule` của DAG."""
    if raw is None:
        return None

    if isinstance(raw, str):
        cron = raw.strip()
        if not cron:
            raise ValueError("schedule là chuỗi rỗng")
        return cron

    if isinstance(raw, dict):
        unknown = set(raw) - ALLOWED_SCHEDULE_KEYS
        if unknown:
            raise ValueError(
                f"schedule có khoá lạ: {sorted(unknown)}. "
                f"Chỉ chấp nhận: {sorted(ALLOWED_SCHEDULE_KEYS)}"
            )
        cron = raw.get("cron")
        if not isinstance(cron, str) or not cron.strip():
            raise ValueError("schedule dạng object bắt buộc có khoá 'cron' là chuỗi")

        timezone = raw.get("timezone")
        if timezone is None:
            return cron.strip()
        if not isinstance(timezone, str):
            raise ValueError("schedule.timezone phải là chuỗi, ví dụ Asia/Ho_Chi_Minh")

        # Validate sớm ở lúc parse để sai timezone hiện ngay thành lỗi config,
        # thay vì để scheduler nổ lúc chạy.
        _validate_timezone(timezone)
        # CronMixin.timezone nhận thẳng str nên không cần ép sang pendulum.
        return CronTriggerTimetable(cron.strip(), timezone=timezone)

    raise TypeError(
        f"schedule không hợp lệ: {raw!r}. Phải là chuỗi cron hoặc object {{cron, timezone}}"
    )


def schedule_timezone(raw: Any) -> str | None:
    """Timezone khai báo trong schedule, nếu có. Dùng để canh start_date cho khớp."""
    if isinstance(raw, dict):
        timezone = raw.get("timezone")
        if isinstance(timezone, str):
            return timezone
    return None


def _validate_timezone(name: str) -> None:
    try:
        pendulum.timezone(name)
    except Exception as exc:  # noqa: BLE001 — pendulum ném nhiều loại lỗi khác nhau
        raise ValueError(f"timezone không hợp lệ: {name!r} ({exc})") from exc
