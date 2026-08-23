# =====================================================================
# Nạp biến từ file .env vào session PowerShell hiện tại.
#
# PHẢI dot-source (dấu chấm + khoảng trắng ở đầu) thì biến mới tồn tại
# sau khi script kết thúc:
#
#     . .\scripts\load_env.ps1
#
# Chạy kiểu ".\scripts\load_env.ps1" sẽ KHÔNG có tác dụng.
# =====================================================================

$repoRoot = Split-Path -Parent $PSScriptRoot
$envFile = Join-Path $repoRoot '.env'

if (-not (Test-Path $envFile)) {
    Write-Host "[X] Khong tim thay .env" -ForegroundColor Red
    Write-Host "    Chay:  Copy-Item .env.example .env   roi dien gia tri that." -ForegroundColor Yellow
    return
}

$loaded = 0
foreach ($line in Get-Content $envFile -Encoding UTF8) {
    $trimmed = $line.Trim()

    # Bỏ qua dòng trống và dòng comment
    if ($trimmed -eq '' -or $trimmed.StartsWith('#')) { continue }

    # Tách theo dấu = ĐẦU TIÊN (giá trị có thể chứa dấu = khác)
    $idx = $trimmed.IndexOf('=')
    if ($idx -lt 1) { continue }

    $key = $trimmed.Substring(0, $idx).Trim()
    $value = $trimmed.Substring($idx + 1).Trim()

    # Gỡ cặp nháy bao ngoài nếu có
    if ($value.Length -ge 2 -and
        (($value.StartsWith('"') -and $value.EndsWith('"')) -or
         ($value.StartsWith("'") -and $value.EndsWith("'")))) {
        $value = $value.Substring(1, $value.Length - 2)
    }

    Set-Item -Path "env:$key" -Value $value
    $loaded++
}

# DBT_PROFILES_DIR để tương đối "." dễ sai khi cd sang thư mục khác
# -> quy về đường dẫn tuyệt đối của repo.
$env:DBT_PROFILES_DIR = $repoRoot

Write-Host "[OK] Da nap $loaded bien tu .env" -ForegroundColor Green
Write-Host "     GCP_PROJECT_ID  = $env:GCP_PROJECT_ID"
Write-Host "     BQ_RAW_DATASET  = $env:BQ_RAW_DATASET"
Write-Host "     BQ_DBT_DATASET  = $env:BQ_DBT_DATASET"
Write-Host "     BQ_LOCATION     = $env:BQ_LOCATION"
