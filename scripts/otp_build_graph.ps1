# Build OTP graph into otp/data (run from repo root).
# Requires: otp/data/*.osm.pbf + GTFS zip — see otp/data/README.txt and docs/otp.md

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$data = Join-Path $root "otp/data"

if (-not (Test-Path $data)) {
    New-Item -ItemType Directory -Path $data | Out-Null
}

# OTP 2.x: path yok — mount edilen /var/opentripplanner kullanılır (resmi: --build --save)
docker run --rm -e JAVA_TOOL_OPTIONS=-Xmx8g `
    -v "${data}:/var/opentripplanner" `
    opentripplanner/opentripplanner:2.5.0 `
    --build --save

Write-Host "Done. Start server: docker compose --profile otp up -d otp"
