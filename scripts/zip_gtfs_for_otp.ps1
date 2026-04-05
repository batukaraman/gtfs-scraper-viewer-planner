# Zip repo gtfs/*.txt -> otp/data/gtfs.zip for OpenTripPlanner (flat GTFS root inside zip).
# Run from repo root:  .\scripts\zip_gtfs_for_otp.ps1

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$gtfs = Join-Path $root "gtfs"
$destDir = Join-Path $root "otp/data"
$zip = Join-Path $destDir "gtfs.zip"

if (-not (Test-Path $gtfs)) {
    Write-Error "Klasör yok: $gtfs"
}
$txts = Get-ChildItem -Path $gtfs -Filter "*.txt" -File
if ($txts.Count -eq 0) {
    Write-Error "gtfs içinde .txt yok"
}
New-Item -ItemType Directory -Force -Path $destDir | Out-Null
if (Test-Path $zip) { Remove-Item $zip -Force }
Compress-Archive -Path ($txts.FullName) -DestinationPath $zip -CompressionLevel Optimal
Write-Host "Yazıldı: $zip ($($txts.Count) dosya)"
