# OpenTripPlanner (OTP) entegrasyonu

Bu projede Streamlit planner, `OTP_BASE_URL` tanımlıyken **OpenTripPlanner** ile de çalışabilir. Bu modda Python tarafında `build_raptor_context` **çalışmaz**; rota OTP sunucusundan sorulur (genelde çok daha hızlı).

## Gereksinimler

- Java tabanlı OTP 2.x Docker imajı (compose dosyasında tanımlı).
- **OSM** `.pbf` (yürüyüş/cadde ağı) + **GTFS** zip veya klasör.
- Makinede yeterli **RAM** (İstanbul için genelde **8 GB+** JVM heap önerilir).

## 1. Veriyi `otp/data` içine koyun

`otp/data/README.txt` dosyasına bakın. Özet:

- **OSM:** `*.osm.pbf` **zorunludur** — yalnızca `gtfs.zip` ile grafik genelde kurulamaz veya yürüyüş olmaz.  
  - [Geofabrik Turkey](https://download.geofabrik.de/europe/turkey.html) → `turkey-latest.osm.pbf` (~1 GB) indirip `otp/data/` içine koy.  
  - Otomatik indirme (uzun sürer): repo kökünde `.\scripts\download_turkey_osm_for_otp.ps1`
- **GTFS:** `gtfs.zip` — zip’in **içinde** doğrudan `agency.txt`, `routes.txt`, … (alt klasör `gtfs/` olmadan).

Bu repoda `gtfs/` klasöründe zaten `.txt` dosyaları varsa, repo kökünden:

```powershell
.\scripts\zip_gtfs_for_otp.ps1
```

çıktı: `otp/data/gtfs.zip`. Sonra OSM `.pbf` dosyasını elle `otp/data/` içine indirip koyun.

## 2. Grafik derleme (bir kez veya feed değişince)

Proje kökünden (PowerShell örneği):

```powershell
docker run --rm -e JAVA_TOOL_OPTIONS=-Xmx8g `
  -v "${PWD}/otp/data:/var/opentripplanner" `
  opentripplanner/opentripplanner:2.5.0 `
  --build --save
```

**Not:** `--save` sonrasına dizin yazma; OTP, volume’un bağlandığı `/var/opentripplanner` dizinini kullanır ([resmi örnek](https://docs.opentripplanner.org/en/latest/Container-Image/)). GTFS zip adında **`gtfs` geçmeli** (örn. `gtfs.zip`).

Bu komut `otp/data` altında grafik dosyası üretir. İmaj sürümü `docker-compose.yml` ile aynı olmalı.

## 3. OTP sunucusunu başlatın

```bash
docker compose --profile otp up -d otp
```

Compose varsayılanı **8081** (8080 çoğu uygulamayla çakışmasın diye). `.env`:

```env
OTP_BASE_URL=http://localhost:8081
OTP_PORT=8081
```

8080 boşsa `OTP_PORT=8080` kullanabilirsin.

## 4. Streamlit planner

```bash
python -m planner
```

Kenar çubukta **Rota motoru → OpenTripPlanner** seçin. Şu an yalnızca **iki waypoint (A→B)** OTP ile desteklenir; çoklu durak için **Dahili Python** kullanın.

GraphQL uç noktası: `{OTP_BASE_URL}/otp/routers/default/index/graphql`

## Sorun giderme

- **Boş itinerary**: Tarih/saat servis dışında, grafik eksik veya OSM/GTFS kapsamı uyuşmuyor olabilir.
- **GraphQL hata**: OTP sürümünüz `plan` şemasını değiştirdiyse `planner/otp_client.py` içindeki sorguyu güncellemeniz gerekir.
- **Bellek**: `JAVA_TOOL_OPTIONS=-Xmx12g` gibi artırın.

Resmi dokümantasyon: [OpenTripPlanner](https://www.opentripplanner.org/).
