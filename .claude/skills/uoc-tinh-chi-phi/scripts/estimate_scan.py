"""Ước tính bytes quét của model dbt bằng dry run (miễn phí), hoặc đọc số thật đã chạy.

    python .claude/skills/uoc-tinh-chi-phi/scripts/estimate_scan.py --dry-run
    python .claude/skills/uoc-tinh-chi-phi/scripts/estimate_scan.py --select fct_orders --run
    python .claude/skills/uoc-tinh-chi-phi/scripts/estimate_scan.py --actual

Dry run KHÔNG thực thi query và KHÔNG bị tính tiền.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]
GIB = 2**30
TIB = 2**40


def gia_moi_tb() -> float:
    """Đơn giá on-demand khác nhau theo region — cho phép ghi đè bằng env."""
    return float(os.getenv("BQ_GIA_MOI_TB", "6.25"))


def tim_sql(thu_muc: str, select: str | None) -> list[Path]:
    goc = REPO_ROOT / "target" / thu_muc
    if not goc.exists():
        sys.exit(f"[X] Chua co {goc}. Chay 'dbt compile' truoc.")
    files = [f for f in goc.rglob("*.sql") if "models" in f.parts]
    if select:
        files = [f for f in files if f.stem == select]
        if not files:
            sys.exit(f"[X] Khong tim thay model '{select}' trong target/{thu_muc}/")
    return sorted(files)


def dry_run(files: list[Path]) -> None:
    from google.cloud import bigquery

    project = os.getenv("GCP_PROJECT_ID")
    if not project:
        sys.exit("[X] Thieu GCP_PROJECT_ID. Chay:  . .\scripts\load_env.ps1")

    client = bigquery.Client(project=project, location=os.getenv("BQ_LOCATION"))
    cfg = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    gia = gia_moi_tb()

    print(f"{'MODEL':<34}{'GiB QUET':>12}{'USD':>10}")
    print("-" * 56)
    tong = 0
    for f in files:
        sql = f.read_text(encoding="utf-8").strip()
        if not sql:
            continue
        try:
            b = client.query(sql, job_config=cfg).total_bytes_processed
            tong += b
            print(f"{f.stem:<34}{b / GIB:>12.4f}{b / TIB * gia:>10.4f}")
        except Exception as e:
            print(f"{f.stem:<34}{'LOI':>12}   {str(e).splitlines()[0][:40]}")

    print("-" * 56)
    print(f"{'TONG':<34}{tong / GIB:>12.4f}{tong / TIB * gia:>10.4f}")
    print(f"\n[!] Model incremental bi UOC TINH THAP o che do --compiled:")
    print("    SQL compile chi la phan SELECT, luc chay con MERGE quet ca bang dich.")
    print("    Dung --run de lay SQL that.")


def doc_so_that() -> None:
    rr_path = REPO_ROOT / "target" / "run_results.json"
    if not rr_path.exists():
        sys.exit("[X] Chua co target/run_results.json. Chay 'dbt run' truoc.")

    rr = json.loads(rr_path.read_text(encoding="utf-8"))
    gia = gia_moi_tb()
    print(f"Lan chay: {rr['metadata']['generated_at']}\n")
    print(f"{'MODEL':<34}{'PROCESSED':>14}{'BILLED':>14}{'USD':>9}")
    print("-" * 71)

    tp = tb = 0
    for r in rr["results"]:
        ar = r.get("adapter_response") or {}
        if not ar:
            continue
        p, b = ar.get("bytes_processed") or 0, ar.get("bytes_billed") or 0
        tp, tb = tp + p, tb + b
        print(f"{r['unique_id'].split('.')[-1]:<34}{p:>14,}{b:>14,}{b / TIB * gia:>9.4f}")

    print("-" * 71)
    print(f"{'TONG':<34}{tp:>14,}{tb:>14,}{tb / TIB * gia:>9.4f}")
    if tp and tb / tp > 2:
        print(f"\n[!] billed gap {tb / tp:.1f} lan processed.")
        print("    BigQuery tinh toi thieu 10 MB moi bang duoc tham chieu.")
        print("    Luon quy ra tien bang bytes_billed, khong phai bytes_processed.")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--select", help="Chi mot model")
    ap.add_argument("--run", action="store_true",
                    help="Lay SQL tu target/run/ (co MERGE) thay vi target/compiled/")
    ap.add_argument("--actual", action="store_true",
                    help="Doc so THAT tu run_results.json thay vi dry run")
    args = ap.parse_args()

    if args.actual:
        doc_so_that()
    else:
        dry_run(tim_sql("run" if args.run else "compiled", args.select))


if __name__ == "__main__":
    main()
