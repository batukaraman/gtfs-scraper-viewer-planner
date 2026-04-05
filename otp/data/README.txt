OpenTripPlanner graph klasörü
=============================

Bu dizine şunları koyun, sonra docs/otp.md içindeki "Grafik derleme" adımını çalıştırın:

  - Bölge OSM: *.osm.pbf (ZORUNLU — sadece gtfs.zip yetmez)
    İndir: https://download.geofabrik.de/europe/turkey.html  -> turkey-latest.osm.pbf
    veya repo kökünden:  .\scripts\download_turkey_osm_for_otp.ps1
    Sadece İstanbul (OTP bellek/küçük grafik): önce turkey pbf, sonra
      .\scripts\extract_istanbul_osm_for_otp.ps1 -ArchiveTurkey
    (turkey dosyasını .archived yapar; otp/data içinde tek .pbf kalsın.)
  - GTFS: gtfs.zip (zip içinde kökte agency.txt, routes.txt, ...)

Projede zaten gtfs/*.txt varsa — OTP öncesi bütünlük (önerilir, ~1–3 dk):

  python scripts/validate_and_fix_gtfs.py

Sonra zip (repo kökünden):

  PowerShell:  .\scripts\zip_gtfs_for_otp.ps1
  Bash:        ./scripts/zip_gtfs_for_otp.sh

Derleme başarılı olunca burada graph.obj (veya sürüme göre benzeri) oluşur.
Aynı klasör hem build hem `docker compose --profile otp up` ile servis için kullanılır.
