"""PostToolUse hook: chay sqlfluff lint tren file .sql vua sua.

Chay ASYNC vi templater = dbt phai compile ca project -> ~10 giay moi file.
Chi LINT, khong bao gio 'fix': repo co y can le 'as' cho de doc, sqlfluff fix
se pha sach style do.
"""

import json
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SQLFLUFF = REPO_ROOT / ".venv" / "Scripts" / "sqlfluff.exe"
THU_MUC_LINT = ("models", "tests", "analyses")


def main() -> None:
    try:
        data = json.load(sys.stdin)
    except Exception:
        return

    raw = ((data.get("tool_response") or {}).get("filePath")
           or (data.get("tool_input") or {}).get("file_path", ""))
    if not raw:
        return

    f = Path(raw)
    if f.suffix.lower() != ".sql":
        return
    try:
        rel = f.resolve().relative_to(REPO_ROOT)
    except ValueError:
        return  # file ngoai repo
    if rel.parts[0] not in THU_MUC_LINT:
        return
    if not SQLFLUFF.exists():
        return  # chua tao venv thi bo qua, khong lam phien

    r = subprocess.run([str(SQLFLUFF), "lint", str(f)],
                       capture_output=True, text=True, cwd=REPO_ROOT)
    if r.returncode == 0:
        return  # sach

    print(json.dumps({
        "hookSpecificOutput": {
            "hookEventName": "PostToolUse",
            "additionalContext": f"sqlfluff bao loi trong {rel}:\n{r.stdout[-3000:]}",
        }
    }))


if __name__ == "__main__":
    main()
