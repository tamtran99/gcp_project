"""Xoá dataset BigQuery của các Pull Request đã đóng.

DAG `git_pr_builds` build mỗi PR vào một bộ dataset riêng (`pr_42_staging`,
`pr_42_marts`...). Không dọn thì chúng tồn tại vĩnh viễn và vẫn tính tiền lưu trữ.

Script hỏi GitHub danh sách PR đang mở, rồi xoá mọi dataset `pr_<so>_*` không
thuộc PR nào còn mở. Chạy lại nhiều lần đều an toàn.

Chay:
  python scripts/cleanup_pr_datasets.py --github-repo tamtran99/gcp_project
  python scripts/cleanup_pr_datasets.py --dry-run
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.request

from google.api_core import exceptions as gcp_exceptions
from google.cloud import bigquery

from _env import LOCATION, PROJECT_ID

# pr_42, pr_42_staging, pr_42_marts...
PR_DATASET_RE = re.compile(r"^pr_(\d+)(?:_.*)?$")


def fetch_open_pr_numbers(github_repo: str) -> set[str]:
    """Số hiệu các PR đang mở. Token là tuỳ chọn (repo public vẫn gọi được)."""
    url = f"https://api.github.com/repos/{github_repo}/pulls?state=open&per_page=100"
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "cleanup-pr-datasets",
    }
    token = os.getenv("GITHUB_TOKEN", "").strip()
    if token:
        headers["Authorization"] = f"Bearer {token}"

    request = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        # Không xác định được PR nào còn mở thì TUYỆT ĐỐI không xoá gì cả —
        # xoá nhầm dataset của PR đang chạy còn tệ hơn là để rác lại.
        sys.exit(f"[X] Goi GitHub API that bai ({exc.code}): {exc.reason}")
    except urllib.error.URLError as exc:
        sys.exit(f"[X] Khong ket noi duoc GitHub: {exc.reason}")

    return {str(pull["number"]) for pull in payload}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--github-repo",
        default=os.getenv("GITHUB_REPO", "tamtran99/gcp_project"),
        help="Repo dang owner/name",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Chi liet ke dataset se xoa, khong xoa that",
    )
    args = parser.parse_args()

    open_prs = fetch_open_pr_numbers(args.github_repo)
    print(f"PR dang mo: {sorted(open_prs, key=int) or '(khong co)'}\n")

    client = bigquery.Client(project=PROJECT_ID, location=LOCATION)

    to_delete: list[str] = []
    for dataset in client.list_datasets(project=PROJECT_ID):
        name = dataset.dataset_id
        match = PR_DATASET_RE.match(name)
        if not match:
            continue
        pr_number = match.group(1)
        if pr_number in open_prs:
            print(f"  [=] {name:<28} PR #{pr_number} con mo, giu lai")
            continue
        to_delete.append(name)

    if not to_delete:
        print("Khong co dataset nao can xoa.")
        return

    print()
    for name in to_delete:
        if args.dry_run:
            print(f"  [~] {name:<28} se bi xoa (dry-run)")
            continue
        try:
            client.delete_dataset(
                f"{PROJECT_ID}.{name}", delete_contents=True, not_found_ok=True
            )
            print(f"  [-] {name:<28} da xoa")
        except gcp_exceptions.Forbidden as exc:
            print(f"  [!] {name:<28} khong du quyen xoa: {exc}")

    print(f"\nXong. {len(to_delete)} dataset.")


if __name__ == "__main__":
    main()
