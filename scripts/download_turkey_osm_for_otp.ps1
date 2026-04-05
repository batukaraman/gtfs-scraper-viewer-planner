# İndirir: Geofabrik turkey-latest.osm.pbf -> otp/data/ (~1 GB civarı, süre ağa bağlı).
# Çalıştır: repo kökünden  .\scripts\download_turkey_osm_for_otp.ps1

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$destDir = Join-Path $root "otp/data"
$url = "https://download.geofabrik.de/europe/turkey-latest.osm.pbf"
$out = Join-Path $destDir "turkey-latest.osm.pbf"

New-Item -ItemType Directory -Force -Path $destDir | Out-Null
if (Test-Path $out) {
    Write-Host "Dosya zaten var, atlanıyor: $out"
    exit 0
}

Write-Host "İndiriliyor (uzun sürebilir): $url"
Write-Host "Hedef: $out"
Invoke-WebRequest -Uri $url -OutFile $out -UseBasicParsing
Write-Host "Tamam."
