"""PreToolUse hook: buoc xac nhan truoc cac lenh dbt ton tien BigQuery.

Khong chan cung - chi tra permissionDecision "ask" kem ly do, vi doi khi
--full-refresh dung la thu can lam.

Vi sao dung hook thay vi permission rule: rule khop theo TIEN TO, nen
"Bash(dbt run --full-refresh*)" se bo lot "dbt run --select x --full-refresh".
Hook doc ca cau lenh nen bat duoc co o bat ky vi tri nao.
"""

import json
import sys


def main() -> None:
    try:
        data = json.load(sys.stdin)
    except Exception:
        return  # khong parse duoc thi de nguyen luong binh thuong

    cmd = (data.get("tool_input") or {}).get("command", "")
    if not cmd:
        return

    ly_do = []
    if "--full-refresh" in cmd:
        ly_do.append(
            "Co --full-refresh: build lai TOAN BO bang incremental tu dau. "
            "Day la khoan quet du lieu ton tien nhat trong mot project dbt."
        )

    la_lenh_build = any(
        f"dbt {v}" in cmd for v in ("run", "build")
    ) and "--help" not in cmd
    if la_lenh_build and "--select" not in cmd and "-s " not in cmd:
        ly_do.append(
            "Khong co --select: se build lai CA PROJECT thay vi phan dang sua. "
            "Neu chi muon kiem tra mot model, them --select <ten_model>."
        )

    if ly_do:
        print(json.dumps({
            "hookSpecificOutput": {
                "hookEventName": "PreToolUse",
                "permissionDecision": "ask",
                "permissionDecisionReason": " | ".join(ly_do),
            }
        }))


if __name__ == "__main__":
    main()
