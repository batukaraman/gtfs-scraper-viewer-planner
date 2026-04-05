# turkey-latest.osm.pbf dosyasından İstanbul metropol alanı (+ tampon) bbox kesimi üretir.
# osmium için Docker kullanır (yerel osmium kurulumu gerekmez).
#
# Önkoşul: otp/data/turkey-latest.osm.pbf (download_turkey_osm_for_otp.ps1 ile veya elle)
# Çalıştır (repo kökünden):  .\scripts\extract_istanbul_osm_for_otp.ps1
#
# OTP bu dizindeki TÜM .pbf dosyalarını okur. Sadece İstanbul kullanacaksanız
# -ArchiveTurkey ile tam Türkiye dosyasını .archived uzantısıyla yeniden adlandırın
# veya elle taşıyın/silin.
#
# Kesim: Debian slim konteynerinde apt ile osmium-tool (Alpine'da paket yok).

param(
    [string] $InputFile = "turkey-latest.osm.pbf",
    [string] $OutputFile = "istanbul.osm.pbf",
    [string] $Bbox = "27.90,40.72,30.12,41.58",
    [switch] $ArchiveTurkey,
    [string] $DockerImage = "debian:bookworm-slim"
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$dataDir = Join-Path $root "otp/data"
$inPath = Join-Path $dataDir $InputFile
$outPath = Join-Path $dataDir $OutputFile

if (-not (Test-Path $inPath)) {
    Write-Error "Bulunamadi: $inPath`nOnce: .\scripts\download_turkey_osm_for_otp.ps1"
}

New-Item -ItemType Directory -Force -Path $dataDir | Out-Null

Write-Host "Docker: $DockerImage (ilk calismada apt + osmium-tool kurulur, 1-2 dk surebilir)"
docker pull $DockerImage | Out-Host

$dataAbs = (Resolve-Path $dataDir).Path
Write-Host "Kesim: $InputFile -> $OutputFile"
Write-Host "Bbox (min_lon,min_lat,max_lon,max_lat): $Bbox"

$inner = "apt-get update -qq && apt-get install -y -qq osmium-tool && osmium extract -b $Bbox --strategy complete_ways --overwrite /data/$InputFile -o /data/$OutputFile"
docker run --rm -v "${dataAbs}:/data" $DockerImage bash -c "$inner"

if (-not (Test-Path $outPath)) {
    Write-Error "Cikti olusmadi: $outPath"
}

$len = (Get-Item $outPath).Length / 1MB
# Parantezsiz "..." -f $len Write-Host ile birlikte -ForegroundColor sanilir
Write-Host ("Tamam: $outPath ({0:N1} MB)" -f $len)

if ($ArchiveTurkey -and (Test-Path $inPath)) {
    $arch = Join-Path $dataDir ($InputFile + ".archived")
    if (Test-Path $arch) { Remove-Item -Force $arch }
    Rename-Item -Path $inPath -NewName (Split-Path $arch -Leaf)
    Write-Host "Arsivlendi (OTP artik bu dosyayi okumaz): $arch"
}
else {
    Write-Warning "otp/data icinde hem $InputFile hem $OutputFile kaliyorsa OTP IKISINI de yukler.`nSadece Istanbul icin -ArchiveTurkey kullanin veya turkey dosyasini tasiyin."
}
