#!/usr/bin/env bash
# =====================================================================
# Dựng Airflow 3 cho repo này bên trong WSL2 Ubuntu. Chạy lại nhiều lần an toàn.
#
#   bash scripts/airflow_bootstrap.sh              # cài đặt đầy đủ
#   bash scripts/airflow_bootstrap.sh --sync-dags  # chỉ cập nhật DAG bootstrap
#   bash scripts/airflow_bootstrap.sh --start      # cài xong rồi bật UI
#
# ĐIỀU KIỆN TRƯỚC (tự làm trong PowerShell admin, có reboot):
#   wsl --install -d Ubuntu-24.04
#
# Sau lần chạy đầu bạn PHẢI mở ~/airflow/airflow-env.sh điền giá trị thật cho
# GCP_PROJECT_ID và đặt file service account key vào ~/secrets/.
# =====================================================================

set -Eeuo pipefail

AIRFLOW_VERSION="3.3.1"
# Python: dùng bản có sẵn của distro, KHÔNG ghim cứng 3.11.
#
# Máy ARM64 (Snapdragon X) không dùng được deadsnakes PPA — PPA đó chỉ build
# amd64/i386. Ubuntu 24.04 ship sẵn Python 3.12, mà cả Airflow 3.3.1
# (>=3.10, !=3.15) lẫn dbt-core 1.12 (>=3.10, tới 3.14) đều chạy tốt trên đó.
# dbt-extractor 0.6.0 có sẵn wheel manylinux_2_17_aarch64 nên không phải
# biên dịch Rust. PYTHON_VERSION và CONSTRAINTS được suy ra ở bước 1.
PY_BIN="${PY_BIN:-}"
if [[ -z "$PY_BIN" ]]; then
  for cand in python3.12 python3.11 python3.13 python3; do
    if command -v "$cand" >/dev/null 2>&1; then PY_BIN="$(command -v "$cand")"; break; fi
  done
fi

REPO_DIR="${GCP_REPO_DIR:-$HOME/repos/gcp_project}"
REMOTE_URL="${GIT_REMOTE_URL:-https://github.com/tamtran99/gcp_project.git}"
GIT_REF="${GIT_REF:-main}"          # nhanh muon dung; doi khi can test PR truoc khi merge
AIRFLOW_HOME_DIR="${AIRFLOW_HOME:-$HOME/airflow}"
AIRFLOW_VENV="$HOME/venvs/airflow"
DBT_VENV="$HOME/venvs/dbt"
ENV_FILE="$AIRFLOW_HOME_DIR/airflow-env.sh"
BOOTSTRAP_DAGS="$AIRFLOW_HOME_DIR/dags_bootstrap"

log() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }
warn() { printf '\033[1;33m[!] %s\033[0m\n' "$*"; }

# --- Chỉ đồng bộ DAG bootstrap ---------------------------------------------

sync_bootstrap_dags() {
  log "Cập nhật DAG bootstrap"
  mkdir -p "$BOOTSTRAP_DAGS"
  # COPY chứ không symlink. Symlink sẽ trỏ ngược vào thư mục bị `git reset
  # --hard`, nghĩa là một commit hỏng trên main có thể vô hiệu hoá luôn chính
  # cơ chế kéo code về để sửa.
  cp -f "$REPO_DIR"/airflow_bootstrap/*.py "$BOOTSTRAP_DAGS/"
  echo "    -> $BOOTSTRAP_DAGS"
  ls -1 "$BOOTSTRAP_DAGS"
}

start_airflow() {
  # shellcheck source=/dev/null
  source "$ENV_FILE"
  log "Khởi động Airflow — UI tại http://localhost:8080"
  echo "Mật khẩu admin: $AIRFLOW_HOME_DIR/simple_auth_manager_passwords.json.generated"
  # standalone tự ép LocalExecutor và chạy đủ api-server + scheduler +
  # dag-processor + triggerer trong một tiến trình.
  exec "$AIRFLOW_VENV/bin/airflow" standalone
}

case "${1:-}" in
  --sync-dags)
    sync_bootstrap_dags
    exit 0
    ;;
  --start)
    [[ -f "$ENV_FILE" ]] || { warn "Chưa có $ENV_FILE — chạy script không tham số trước."; exit 1; }
    start_airflow
    ;;
esac

# --- 1. Kiểm tra Python ----------------------------------------------------

log "Kiểm tra Python và gói hệ thống"

# Chỉ gọi sudo khi THẬT SỰ thiếu gói. Nhờ vậy script chạy trót lọt với user
# thường trên máy đã cài sẵn (ví dụ WSL vừa được dựng bằng quyền root), không
# bị treo ở prompt mật khẩu sudo khi chạy không tương tác.
need_apt=0
for c in git curl cc; do
  command -v "$c" >/dev/null 2>&1 || need_apt=1
done
if [[ -z "$PY_BIN" ]] || ! "$PY_BIN" -c 'import sys; sys.exit(0 if sys.version_info[:2] >= (3,10) else 1)' 2>/dev/null; then
  need_apt=1
elif ! "$PY_BIN" -m venv --help >/dev/null 2>&1; then
  # python3-venv là gói riêng trên Debian/Ubuntu; thiếu nó thì `python -m venv`
  # fail với thông báo rất khó hiểu.
  need_apt=1
fi

if (( need_apt )); then
  warn "Thiếu gói hệ thống, cần quyền sudo để cài"
  sudo apt-get update
  sudo apt-get install -y git curl build-essential libssl-dev libffi-dev pkg-config
  sudo apt-get install -y python3 python3-venv python3-dev
  PY_BIN="${PY_BIN:-$(command -v python3)}"
else
  echo "    gói hệ thống đã đủ, không cần sudo"
fi

PYTHON_VERSION="$("$PY_BIN" -c 'import sys; print("%d.%d" % sys.version_info[:2])')"
CONSTRAINTS="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
echo "    $PY_BIN -> $("$PY_BIN" --version) trên $(uname -m)"
echo "    constraints: $CONSTRAINTS"

# Cài Airflow không constraint gần như chắc chắn vỡ dependency. Thà dừng sớm.
if ! curl -sfI "$CONSTRAINTS" >/dev/null; then
  warn "Airflow $AIRFLOW_VERSION không có constraints cho Python $PYTHON_VERSION."
  exit 1
fi

# --- 2. Clone repo ---------------------------------------------------------

log "Chuẩn bị clone tại $REPO_DIR (ref: $GIT_REF)"
if [[ ! -d "$REPO_DIR/.git" ]]; then
  mkdir -p "$(dirname "$REPO_DIR")"
  git clone --branch "$GIT_REF" "$REMOTE_URL" "$REPO_DIR"
else
  echo "    clone đã có, giữ nguyên nhánh $(git -C "$REPO_DIR" rev-parse --abbrev-ref HEAD)"
fi

# Clone nhầm nhánh chưa có phần Airflow là lỗi rất dễ mắc mà lại khó hiểu:
# bootstrap chạy xong sạch sẽ nhưng UI Airflow trống trơn, không DAG nào cả.
for must in dags/dag_builder.py airflow_config airflow_bootstrap; do
  if [[ ! -e "$REPO_DIR/$must" ]]; then
    warn "Không thấy $must trong $REPO_DIR (đang ở nhánh $(git -C "$REPO_DIR" rev-parse --abbrev-ref HEAD))."
    warn "Nhánh này chưa có phần Airflow. Đặt GIT_REF=<nhánh> rồi chạy lại."
    exit 1
  fi
done
echo "    OK: dags/, airflow_config/, airflow_bootstrap/ đều có mặt"

# --- 3. Hai venv TÁCH BIỆT -------------------------------------------------

log "Tạo venv Airflow tại $AIRFLOW_VENV"
[[ -d "$AIRFLOW_VENV" ]] || "$PY_BIN" -m venv "$AIRFLOW_VENV"
"$AIRFLOW_VENV/bin/pip" install --quiet --upgrade pip
"$AIRFLOW_VENV/bin/pip" install "apache-airflow==${AIRFLOW_VERSION}" \
  --constraint "$CONSTRAINTS"

log "Tạo venv dbt tại $DBT_VENV"
[[ -d "$DBT_VENV" ]] || "$PY_BIN" -m venv "$DBT_VENV"
"$DBT_VENV/bin/pip" install --quiet --upgrade pip
"$DBT_VENV/bin/pip" install --quiet -r "$REPO_DIR/requirements.txt"

# Hai venv lẫn nhau là nguồn gốc của những lỗi import rất khó hiểu về sau.
if "$AIRFLOW_VENV/bin/pip" show dbt-core >/dev/null 2>&1; then
  warn "venv Airflow có dbt-core — sai. Xoá $AIRFLOW_VENV rồi chạy lại."
  exit 1
fi

# --- 4. Thư mục ------------------------------------------------------------

log "Tạo thư mục"
mkdir -p "$AIRFLOW_HOME_DIR" "$BOOTSTRAP_DAGS" "$AIRFLOW_HOME_DIR/dbt_state/prod" \
         "$HOME/repos/pr_worktrees" "$HOME/secrets"
chmod 700 "$HOME/secrets"

# --- 5. File biến môi trường ----------------------------------------------

if [[ -f "$ENV_FILE" ]]; then
  log "Giữ nguyên $ENV_FILE (đã có)"
else
  log "Sinh $ENV_FILE — BẠN PHẢI ĐIỀN GCP_PROJECT_ID"
  cat >"$ENV_FILE" <<EOF
# Nạp trước khi chạy bất kỳ lệnh airflow nào:  source $ENV_FILE

# ---- Airflow ----
export AIRFLOW_HOME="$AIRFLOW_HOME_DIR"
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__PARALLELISM=8
export AIRFLOW__CORE__DEFAULT_TIMEZONE=Asia/Ho_Chi_Minh
export AIRFLOW__API__PORT=8080
# Parse lại file DAG mỗi 30 giây BẤT KỂ mtime. Quan trọng: sửa
# airflow_config/*.yml không làm đổi mtime của dags/dag_builder.py, nên nếu chỉ
# dựa vào mtime thì thay đổi lịch chạy sẽ không bao giờ được nhận.
export AIRFLOW__DAG_PROCESSOR__MIN_FILE_PROCESS_INTERVAL=30
export AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL=60

# Hai bundle riêng biệt. Đặt biến này THAY THẾ HOÀN TOÀN bundle mặc định
# "dags-folder". Bundle bootstrap nằm ngoài repo nên git reset không đụng tới.
export AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST='[{"name":"bootstrap","classpath":"airflow.dag_processing.bundles.local.LocalDagBundle","kwargs":{"path":"$BOOTSTRAP_DAGS","refresh_interval":300}},{"name":"gcp-project","classpath":"airflow.dag_processing.bundles.local.LocalDagBundle","kwargs":{"path":"$REPO_DIR/dags","refresh_interval":30}}]'

# BẮT BUỘC: \`airflow standalone\` sinh api-server / scheduler / dag-processor /
# triggerer bằng cách gọi lệnh \`airflow\` TRẦN chứ không phải đường dẫn tuyệt
# đối. Thiếu venv bin trên PATH thì cả bốn thread chết ngay với
# "FileNotFoundError: [Errno 2] No such file or directory: 'airflow'", còn tiến
# trình cha vẫn sống nên nhìn như đang chạy mà không có gì listen cổng 8080.
# Nối vào CUỐI PATH để không che python3 của hệ thống.
export PATH="\$PATH:$AIRFLOW_VENV/bin"

# ---- Đường dẫn dùng chung cho DAG factory ----
export GCP_REPO_DIR="$REPO_DIR"
export GIT_REMOTE_URL="$REMOTE_URL"
export GITHUB_REPO="tamtran99/gcp_project"
export DBT_BIN="$DBT_VENV/bin/dbt"
export DBT_PY="$DBT_VENV/bin/python"
export AIRFLOW_BIN="$AIRFLOW_VENV/bin/airflow"
export PR_WORKTREE_ROOT="\$HOME/repos/pr_worktrees"
export GCP_REPO_POOL=gcp_project_repo
export GCP_REPO_POOL_SLOTS=8

# ---- dbt / BigQuery — ĐIỀN GIÁ TRỊ THẬT ----
# PHẢI là đường dẫn tuyệt đối. .env của repo đặt DBT_PROFILES_DIR=. mà
# BashOperator chạy ở thư mục tạm -> dbt sẽ báo "Could not find profile".
export DBT_PROFILES_DIR="$REPO_DIR"
export GCP_PROJECT_ID=DIEN-PROJECT-ID-CUA-BAN
export BQ_LOCATION=asia-southeast1
export BQ_RAW_DATASET=raw_ecommerce
export BQ_DBT_DATASET=dbt_dev
export BQ_PROD_DATASET=analytics
export BQ_CI_DATASET=ci_pr
export DBT_THREADS=8

# dbt đọc GCP_SA_KEYFILE, còn scripts/*.py dùng bigquery.Client() nên đọc
# GOOGLE_APPLICATION_CREDENTIALS. Hai biến PHẢI trỏ cùng một file, nếu không sẽ
# ra lỗi lệch rất khó hiểu: dbt chạy ngon còn script nạp CSV thì DefaultCredentialsError.
export GCP_SA_KEYFILE="\$HOME/secrets/gcp-sa-airflow.json"
export GOOGLE_APPLICATION_CREDENTIALS="\$HOME/secrets/gcp-sa-airflow.json"

# Bắt buộc: file .sql/.yml trong repo có tiếng Việt
export PYTHONUTF8=1
export LANG=C.UTF-8
export LC_ALL=C.UTF-8
EOF
fi

# shellcheck source=/dev/null
source "$ENV_FILE"

# --- 6. Khởi tạo metadata DB ----------------------------------------------

log "Khởi tạo metadata DB (airflow db migrate)"
"$AIRFLOW_VENV/bin/airflow" db migrate

# --- 7. Pool đóng vai mutex trên clone ------------------------------------

log "Tạo pool $GCP_REPO_POOL ($GCP_REPO_POOL_SLOTS slot)"
"$AIRFLOW_VENV/bin/airflow" pools set "$GCP_REPO_POOL" "$GCP_REPO_POOL_SLOTS" \
  "Mutex tren clone dung chung: task dbt lay 1 slot, git reset lay het"

# --- 8. DAG bootstrap ------------------------------------------------------

sync_bootstrap_dags

# --- 9. dbt deps -----------------------------------------------------------

log "Cài dbt packages (bắt buộc trước lần build đầu)"
"$DBT_BIN" --no-use-colors deps --project-dir "$REPO_DIR" || \
  warn "dbt deps lỗi — kiểm tra lại mạng rồi chạy tay"

# --- Xong ------------------------------------------------------------------

log "Xong phần tự động"
cat <<EOF

Còn lại phải làm tay:

  1. Sửa $ENV_FILE, điền GCP_PROJECT_ID thật.

  2. Tạo service account và tải key về ~/secrets/gcp-sa-airflow.json :

       gcloud iam service-accounts create airflow-dbt --display-name="Airflow dbt runner"
       SA="airflow-dbt@\$GCP_PROJECT_ID.iam.gserviceaccount.com"
       # bigquery.user chứ không phải jobUser: PR build cần quyền TẠO dataset
       gcloud projects add-iam-policy-binding "\$GCP_PROJECT_ID" \\
         --member="serviceAccount:\$SA" --role="roles/bigquery.user"
       gcloud projects add-iam-policy-binding "\$GCP_PROJECT_ID" \\
         --member="serviceAccount:\$SA" --role="roles/bigquery.dataEditor"
       gcloud iam service-accounts keys create ~/secrets/gcp-sa-airflow.json --iam-account="\$SA"
       chmod 600 ~/secrets/gcp-sa-airflow.json

  3. Kiểm tra dbt trước khi động vào Airflow:

       source $ENV_FILE
       "\$DBT_BIN" debug --target prod

  4. (Tuỳ chọn) Token GitHub để tránh rate limit khi poll PR:

       "$AIRFLOW_VENV/bin/airflow" variables set github_token '<token>'

  5. Bật UI:

       bash $REPO_DIR/scripts/airflow_bootstrap.sh --start

EOF
