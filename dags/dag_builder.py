"""Điểm vào duy nhất mà dag-processor của Airflow đọc.

Quét `airflow_config/*.yml` và sinh DAG động vào `globals()`.

Chạy trực tiếp để smoke test ngoài Airflow (vẫn cần venv Airflow):

    python dags/dag_builder.py
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

_DAGS_DIR = Path(__file__).resolve().parent

# Thêm tường minh thay vì trông chờ Airflow tự đưa thư mục bundle vào sys.path.
# Nhờ vậy `python dags/dag_builder.py` cũng import được dbt_dag_factory.
if str(_DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(_DAGS_DIR))

from dbt_dag_factory.builder import build_dag, build_error_dag  # noqa: E402
from dbt_dag_factory.config import load_specs  # noqa: E402

log = logging.getLogger(__name__)

for _spec, _error in load_specs():
    if _error is not None:
        # Một file hỏng KHÔNG được làm biến mất toàn bộ DAG còn lại. Thay vào đó
        # sinh một DAG báo lỗi để nhìn thấy ngay trên UI.
        log.error("Lỗi config %s: %s", _error.source, _error.message)
        _dag = build_error_dag(_error)
        globals()[_dag.dag_id] = _dag
        continue

    if _spec.is_disabled:
        # Cố ý KHÔNG dùng is_paused_upon_creation: cờ đó chỉ có tác dụng lần
        # serialize đầu tiên, nên bật is_disabled trên DAG đang chạy sẽ không
        # dừng được nó. Bỏ hẳn DAG mới là idempotent.
        log.info("Bỏ qua %s (is_disabled: true)", _spec.dag_id)
        continue

    globals()[_spec.dag_id] = build_dag(_spec)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    from airflow.sdk import DAG

    # Lọc bằng isinstance chứ KHÔNG bằng hasattr("dag_id"): DagSpec cũng có
    # thuộc tính dag_id, để lọt vào thì vừa đếm sai vừa nổ ở _obj.tasks.
    _dags = {k: v for k, v in dict(globals()).items() if isinstance(v, DAG)}
    print(f"\nĐã dựng {len(_dags)} DAG:")
    for _name, _obj in sorted(_dags.items()):
        # CronTriggerTimetable của Airflow 3.3 để cron ở `expression`, KHÔNG có
        # `summary` hay `description`. Dò lần lượt để không phụ thuộc vào chi
        # tiết nội bộ của một loại timetable cụ thể.
        _tt = _obj.timetable
        _sched = next(
            (
                str(v)
                for v in (getattr(_tt, a, None) for a in ("expression", "summary"))
                if v
            ),
            type(_tt).__name__,
        )
        _tz = getattr(_tt, "timezone", "") or ""
        print(
            f"  - {_obj.dag_id:<24} tasks={len(_obj.tasks):<3} "
            f"schedule={_sched!r} tz={_tz}"
        )
