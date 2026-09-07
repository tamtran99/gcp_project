"""Sinh/cập nhật exposures từ log truy vấn thật của BigQuery.

Ý tưởng: mọi dashboard BI cuối cùng đều thành job trên BigQuery.
INFORMATION_SCHEMA.JOBS ghi lại bảng nào bị đọc và ai đọc, nên ta đọc
ngược từ đó ra thay vì ngồi liệt kê dashboard bằng tay.

Chạy:
    . .\scripts\load_env.ps1
    python scripts\sync_exposures.py --days 90 --dry-run
    python scripts\sync_exposures.py --days 90 --bi-user looker@x.iam.gserviceaccount.com

Lưu ý: script ghi đè file YAML nên COMMENT trong file cũ sẽ mất.
Chạy --dry-run xem trước, và luôn review diff trước khi commit.
"""

from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from pathlib import Path

import yaml
from google.cloud import bigquery

from _env import LOCATION, PROJECT_ID, REPO_ROOT

MANIFEST = REPO_ROOT / "target" / "manifest.json"
DEFAULT_OUT = REPO_ROOT / "models" / "marts" / "_exposures.yml"

# Job do chính dbt/script nạp dữ liệu sinh ra -> không phải người tiêu thụ
IGNORED_QUERY_PREFIXES = ("create or replace", "merge", "insert into", "load data")


def load_model_index() -> tuple[dict[tuple[str, str], str], set[str]]:
    """Đọc manifest, trả về (map (dataset, table) -> tên model, tập model marts).

    Tách riêng tập marts vì hai thứ này phục vụ hai mục đích khác nhau:

    - `index` gồm TẤT CẢ model, dùng để tra ngược từ bảng bị query ra model.
      Ai đó query thẳng vào một view staging thì ta vẫn muốn nhìn thấy.
    - `marts` chỉ dùng cho phần "model không ai query". Chỉ marts mới là thứ BI
      tiêu thụ trực tiếp: staging là view trung gian, còn intermediate mặc định
      là ephemeral nên KHÔNG hề tồn tại trên warehouse. Gộp chúng vào sẽ khiến
      lần chạy nào cũng báo đủ 4 staging + intermediate là "chết".
    """
    if not MANIFEST.exists():
        raise SystemExit(
            "[X] Chua co target/manifest.json. Chay 'dbt compile' hoac 'dbt build' truoc."
        )
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    index: dict[tuple[str, str], str] = {}
    marts: set[str] = set()
    for node in manifest["nodes"].values():
        if node["resource_type"] != "model":
            continue
        index[(node["schema"], node["alias"] or node["name"])] = node["name"]
        # fqn = [ten_project, <thu muc con>..., ten_model]
        if "marts" in node["fqn"][1:-1]:
            marts.add(node["name"])
    return index, marts


def fetch_usage(client: bigquery.Client, days: int, bi_users: list[str]) -> list:
    """Lấy thống kê ai đọc bảng nào trong N ngày qua."""
    user_filter = "and job.user_email in unnest(@bi_users)" if bi_users else ""
    sql = f"""
    select
        job.user_email,
        ref.dataset_id,
        ref.table_id,
        count(*)                                   as so_lan_query,
        max(job.creation_time)                     as lan_cuoi,
        sum(job.total_bytes_processed)             as tong_bytes,
        -- Looker chen comment "Looker Query Context" vao dau cau SQL
        any_value(regexp_extract(job.query, r"--\s*Looker Query Context\s*'([^']*)'")) as looker_context
    from `region-{LOCATION}`.INFORMATION_SCHEMA.JOBS as job,
         unnest(job.referenced_tables) as ref
    where job.creation_time >= timestamp_sub(current_timestamp(), interval @days day)
      and job.job_type = 'QUERY'
      and job.state = 'DONE'
      and job.error_result is null
      and ref.project_id = @project
      {user_filter}
      and not regexp_contains(lower(ltrim(job.query)), r'^({"|".join(IGNORED_QUERY_PREFIXES)})')
    group by 1, 2, 3
    order by so_lan_query desc
    """
    params = [
        bigquery.ScalarQueryParameter("days", "INT64", days),
        bigquery.ScalarQueryParameter("project", "STRING", PROJECT_ID),
        bigquery.ArrayQueryParameter("bi_users", "STRING", bi_users),
    ]
    job = client.query(sql, bigquery.QueryJobConfig(query_parameters=params))
    return list(job.result())


def slugify(email: str) -> str:
    """looker-sa@proj.iam.gserviceaccount.com -> looker_sa"""
    local = email.split("@")[0]
    return re.sub(r"[^a-z0-9]+", "_", local.lower()).strip("_") or "bi_consumer"


def build_exposures(rows, model_index) -> tuple[list[dict], list[tuple]]:
    by_user: dict[str, set[str]] = defaultdict(set)
    unmapped: list[tuple] = []

    for r in rows:
        model = model_index.get((r.dataset_id, r.table_id))
        if model:
            by_user[r.user_email].add(model)
        else:
            unmapped.append((r.dataset_id, r.table_id, r.so_lan_query))

    exposures = []
    for email, models in sorted(by_user.items()):
        exposures.append(
            {
                "name": slugify(email),
                "label": f"Truy van tu {email}",
                "type": "dashboard",
                "maturity": "medium",
                "description": (
                    "TU DONG SINH tu INFORMATION_SCHEMA.JOBS. "
                    "Hay sua lai: tach thanh tung dashboard cu the, dien url va owner that."
                ),
                "owner": {"name": "CAN DIEN", "email": email},
                "depends_on": [f"ref('{m}')" for m in sorted(models)],
            }
        )
    return exposures, unmapped


def merge_existing(new: list[dict], out_path: Path) -> list[dict]:
    """Giữ lại phần đã sửa tay (owner, url, label, maturity, description)."""
    if not out_path.exists():
        return new
    old = yaml.safe_load(out_path.read_text(encoding="utf-8")) or {}
    old_by_name = {e["name"]: e for e in old.get("exposures", [])}

    merged = []
    for exp in new:
        prev = old_by_name.pop(exp["name"], None)
        if prev:
            # depends_on lấy từ log (nguồn sự thật), phần còn lại giữ nguyên
            prev["depends_on"] = exp["depends_on"]
            merged.append(prev)
        else:
            merged.append(exp)

    # Exposure khai báo tay nhưng log không thấy -> giữ lại, chỉ cảnh báo
    for name, orphan in old_by_name.items():
        print(f"[!] '{name}' khai bao trong YAML nhung KHONG co query nao trong cua so thoi gian")
        merged.append(orphan)
    return merged


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=90)
    ap.add_argument("--bi-user", action="append", default=[],
                    help="Email/service account cua BI tool. Bo trong = lay tat ca.")
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    model_index, mart_models = load_model_index()
    print(f"[i] {len(model_index)} model trong manifest, {len(mart_models)} thuoc marts")

    client = bigquery.Client(project=PROJECT_ID, location=LOCATION)
    rows = fetch_usage(client, args.days, args.bi_user)
    print(f"[i] {len(rows)} cap (user, bang) trong {args.days} ngay qua\n")

    exposures, unmapped = build_exposures(rows, model_index)

    for exp in exposures:
        print(f"  {exp['owner']['email']:<45} -> {len(exp['depends_on'])} model")
    if unmapped:
        print("\n[!] Bang bi query nhung KHONG thuoc dbt (nguon thô / bang tay tao):")
        for ds, tbl, n in unmapped[:15]:
            print(f"      {ds}.{tbl}  ({n} query)")

    # Mart khong xuat hien trong log = ung vien xoa.
    # Chi xet marts, khong xet ca manifest: xem giai thich o load_model_index().
    used = {m for e in exposures for m in e["depends_on"]}
    dead = sorted(f"ref('{m}')" for m in mart_models if f"ref('{m}')" not in used)
    if dead:
        print(f"\n[!] {len(dead)}/{len(mart_models)} mart KHONG bi query "
              f"lan nao trong {args.days} ngay:")
        for d in dead:
            print(f"      {d}")

    if args.dry_run:
        print("\n[i] --dry-run: khong ghi file")
        return

    doc = {"version": 2, "exposures": merge_existing(exposures, args.out)}
    args.out.write_text(
        yaml.safe_dump(doc, allow_unicode=True, sort_keys=False, width=100),
        encoding="utf-8",
    )
    print(f"\n[OK] Da ghi {args.out}")


if __name__ == "__main__":
    main()
