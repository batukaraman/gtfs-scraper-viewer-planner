"""Main pipeline orchestration for GTFS scraping and OTP preparation."""

from __future__ import annotations

import csv
import logging
import re
import shutil
import zipfile
from pathlib import Path
from typing import Callable

from .config import CityConfig, CountryConfig, PipelineConfig, load_config
from .gtfs.validator import GTFSValidator
from .gtfs.writer import GTFSWriter
from .osm.downloader import OSMDownloader
from .osm.extractor import OSMExtractor
from .sources.base import GTFSData, GTFSSource
from .sources.easyway import EasyWaySource

logger = logging.getLogger(__name__)


SOURCE_REGISTRY: dict[str, type[GTFSSource]] = {
    "easyway": EasyWaySource,
}


def register_source(source_type: str, source_class: type[GTFSSource]) -> None:
    """Register a new GTFS source type."""
    SOURCE_REGISTRY[source_type] = source_class


class Pipeline:
    """Main pipeline for processing cities."""

    def __init__(
        self,
        config: PipelineConfig | None = None,
        config_path: Path | str | None = None,
        base_dir: Path | str | None = None,
    ):
        if config is None:
            config = load_config(config_path)
        self.config = config

        if base_dir is None:
            base_dir = Path.cwd()
        self.base_dir = Path(base_dir)

        self.data_dir = self.base_dir / "data"
        self.logs_dir = self.base_dir / "logs"

        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)

    def process_city(
        self,
        city_id: str,
        skip_osm: bool = False,
        skip_gtfs: bool = False,
        force: bool = False,
    ) -> dict:
        """Process a single city: scrape GTFS, prepare OSM, setup OTP.
        
        Args:
            city_id: City identifier from config
            skip_osm: Skip OSM download/extract
            skip_gtfs: Skip GTFS scraping
            force: Overwrite existing files
        
        Returns:
            Processing result summary
        """
        city = self.config.get_city(city_id)
        if city is None:
            raise ValueError(f"City not found in config: {city_id}")

        country = self.config.get_country_for_city(city_id)
        if country is None:
            raise ValueError(f"Country not found for city: {city_id}")

        logger.info("=" * 60)
        logger.info("Processing city: %s (%s)", city.name, city.id)
        logger.info("=" * 60)

        result = {
            "city": city_id,
            "country": country.id,
            "gtfs": None,
            "osm": None,
            "city_dir": None,
        }

        # All city data in one place: data/{city}/
        city_dir = self.data_dir / city.id
        city_gtfs_dir = city_dir / "gtfs"

        city_dir.mkdir(parents=True, exist_ok=True)

        if not skip_gtfs:
            gtfs_data = self._scrape_gtfs(city, city_gtfs_dir)
            zip_path = self._write_and_validate_gtfs(city, city_gtfs_dir, gtfs_data)
            result["gtfs"] = str(zip_path)

        if not skip_osm:
            osm_path = self._prepare_osm(country, city, city_dir, force)
            result["osm"] = str(osm_path)

        self._write_otp_configs(city, city_dir)
        result["city_dir"] = str(city_dir)

        logger.info("City processing complete: %s", city.id)
        return result

    def process_all(
        self,
        skip_osm: bool = False,
        skip_gtfs: bool = False,
        force: bool = False,
    ) -> list[dict]:
        """Process all cities in config."""
        results = []
        for city in self.config.all_cities():
            try:
                result = self.process_city(
                    city.id,
                    skip_osm=skip_osm,
                    skip_gtfs=skip_gtfs,
                    force=force,
                )
                results.append(result)
            except Exception as e:
                logger.error("Failed to process %s: %s", city.id, e)
                results.append({
                    "city": city.id,
                    "error": str(e),
                })
        return results

    def _scrape_gtfs(self, city: CityConfig, output_dir: Path) -> GTFSData:
        """Scrape GTFS data from all sources for a city."""
        combined_data = GTFSData()

        for source_config in city.sources:
            source_type = source_config.type

            if source_type not in SOURCE_REGISTRY:
                logger.warning("Unknown source type: %s", source_type)
                continue

            source_class = SOURCE_REGISTRY[source_type]
            source = source_class(
                city_config=city,
                source_config=source_config,
                progress_dir=self.logs_dir,
            )

            logger.info("Scraping from %s source...", source_type)
            data = source.scrape()
            combined_data.merge(data)

            stats = data.stats()
            logger.info("Source stats: %s", stats)

        return combined_data

    def _write_and_validate_gtfs(
        self,
        city: CityConfig,
        gtfs_dir: Path,
        data: GTFSData,
    ) -> Path:
        """Write GTFS files, validate, and create ZIP."""
        writer = GTFSWriter(city, gtfs_dir)
        writer.write(data)

        validator = GTFSValidator(
            gtfs_dir,
            timezone=city.timezone,
            language=city.language,
        )
        validator.validate_and_fix()

        return writer.create_zip()

    def _prepare_osm(
        self,
        country: CountryConfig,
        city: CityConfig,
        city_dir: Path,
        force: bool = False,
    ) -> Path:
        """Download country OSM and extract city bbox."""
        # Shared OSM cache: data/osm/
        osm_cache_dir = self.data_dir / "osm"
        osm_cache_dir.mkdir(parents=True, exist_ok=True)

        downloader = OSMDownloader(osm_cache_dir)
        downloader.download(
            country.osm.source,
            country.osm.filename,
            force=force,
        )

        # Extract directly to city directory
        extractor = OSMExtractor(osm_cache_dir)
        city_osm_path = city_dir / f"{city.id}.osm.pbf"
        
        extractor.extract(
            country.osm.filename,
            city_osm_path.name,
            city.bbox,
            output_dir=city_dir,
            force=force,
        )

        return city_osm_path

    def _write_otp_configs(self, city: CityConfig, city_dir: Path) -> None:
        """Write OTP configuration files."""
        build_config = {
            "transitServiceStart": "-P1Y",
            "transitServiceEnd": "P1Y",
        }

        router_config = {
            "routingDefaults": {
                "walkSpeed": 1.3,
                "bikeSpeed": 5.0,
                "carSpeed": 15.0,
                "numItineraries": 5,
            },
            "updaters": [],
        }

        import json

        with open(city_dir / "build-config.json", "w", encoding="utf-8") as f:
            json.dump(build_config, f, indent=2)

        with open(city_dir / "router-config.json", "w", encoding="utf-8") as f:
            json.dump(router_config, f, indent=2)

        logger.info("OTP config files written to %s", city_dir)

    def build_otp_graph(self, city_id: str, memory: str | None = None) -> bool:
        """Build OTP graph for a city using Docker.
        
        Args:
            city_id: City identifier
            memory: JVM heap size (e.g., "8g"). Uses config default if not specified.
        
        Returns:
            True if build succeeded
        """
        import subprocess

        city = self.config.get_city(city_id)
        if city is None:
            raise ValueError(f"City not found: {city_id}")

        city_dir = self.data_dir / city.id

        if not (city_dir / "gtfs.zip").exists():
            raise FileNotFoundError(f"GTFS not found: {city_dir}/gtfs.zip")

        sanitize_report = self._sanitize_gtfs_for_otp(city_dir)
        if sanitize_report:
            logger.info("GTFS pre-build sanitize report: %s", sanitize_report)

        osm_files = list(city_dir.glob("*.osm.pbf"))
        if not osm_files:
            raise FileNotFoundError(f"No OSM file found in {city_dir}")

        if memory is None:
            memory = city.otp.memory

        logger.info("Building OTP graph for %s (memory: %s)...", city.id, memory)

        cmd = [
            "docker", "run", "--rm",
            "-e", f"JAVA_TOOL_OPTIONS=-Xmx{memory}",
            "-v", f"{city_dir.resolve()}:/var/opentripplanner",
            "opentripplanner/opentripplanner:2.5.0",
            "--build", "--save",
        ]

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=7200,
            )

            if result.returncode != 0:
                logger.error("OTP build failed: %s", result.stderr)
                return False

            graph_path = city_dir / "graph.obj"
            if graph_path.exists():
                size_mb = graph_path.stat().st_size / 1024 / 1024
                logger.info("Graph built: %s (%.1f MB)", graph_path, size_mb)
                return True
            else:
                logger.error("Graph file not created")
                return False

        except subprocess.TimeoutExpired:
            logger.error("OTP build timed out")
            return False

    def _sanitize_gtfs_for_otp(self, city_dir: Path) -> dict[str, dict[str, int]]:
        """Normalize GTFS text files to avoid CSV shape issues before OTP build."""
        gtfs_dir = city_dir / "gtfs"
        if not gtfs_dir.exists():
            logger.warning("GTFS directory not found for sanitize: %s", gtfs_dir)
            return {}

        report: dict[str, dict[str, int]] = {}
        allowed_route_types = {"0", "1", "2", "3", "4", "5", "6", "7", "11", "12"}

        def _normalize_coordinate(raw: str, kind: str) -> str | None:
            """Normalize malformed lat/lon like '410.191.700.005.564' -> '41.0191700005564'."""
            txt = (raw or "").strip().replace(",", ".")
            min_val, max_val = (39.0, 42.5) if kind == "lat" else (26.0, 31.5)

            def _is_valid(v: str) -> bool:
                try:
                    x = float(v)
                except (TypeError, ValueError):
                    return False
                return min_val <= x <= max_val

            if _is_valid(txt):
                return str(float(txt))

            digits = re.sub(r"\D", "", txt)
            if not digits:
                return None

            # Most malformed Istanbul coords recover with 2-digit integer part.
            for split in (2, 3, 1):
                if len(digits) <= split:
                    continue
                candidate = f"{digits[:split]}.{digits[split:]}"
                if _is_valid(candidate):
                    return str(float(candidate))

            return None

        for path in sorted(gtfs_dir.glob("*.txt")):
            with path.open("r", encoding="utf-8", newline="") as f:
                rows = list(csv.reader(f))

            if not rows:
                report[path.name] = {"input_rows": 0, "output_rows": 0, "fixed_width": 0, "dropped": 0}
                continue

            header = rows[0]
            cols = len(header)
            idx = {name: i for i, name in enumerate(header)}

            out = [header]
            fixed_width = 0
            fixed_required = 0
            dropped = 0

            for row in rows[1:]:
                if len(row) > cols:
                    row = row[:cols - 1] + [",".join(row[cols - 1:])]
                    fixed_width += 1
                elif len(row) < cols:
                    row = row + [""] * (cols - len(row))
                    fixed_width += 1

                # file-specific required field repairs
                if path.name == "routes.txt":
                    route_id_i = idx.get("route_id")
                    route_type_i = idx.get("route_type")
                    short_i = idx.get("route_short_name")
                    long_i = idx.get("route_long_name")

                    if route_id_i is not None and not row[route_id_i].strip():
                        dropped += 1
                        continue

                    if route_type_i is not None and row[route_type_i].strip() not in allowed_route_types:
                        row[route_type_i] = "3"
                        fixed_required += 1

                    if (
                        short_i is not None
                        and long_i is not None
                        and not row[short_i].strip()
                        and not row[long_i].strip()
                    ):
                        dropped += 1
                        continue

                elif path.name == "stops.txt":
                    stop_id_i = idx.get("stop_id")
                    stop_name_i = idx.get("stop_name")
                    lat_i = idx.get("stop_lat")
                    lon_i = idx.get("stop_lon")

                    if stop_id_i is not None and not row[stop_id_i].strip():
                        dropped += 1
                        continue

                    if stop_name_i is not None and not row[stop_name_i].strip():
                        row[stop_name_i] = "Unknown Stop"
                        fixed_required += 1

                    if lat_i is not None:
                        lat_val = _normalize_coordinate(row[lat_i], "lat")
                        if lat_val is None:
                            lat_val = "41.0082"
                        if row[lat_i] != lat_val:
                            row[lat_i] = lat_val
                            fixed_required += 1

                    if lon_i is not None:
                        lon_val = _normalize_coordinate(row[lon_i], "lon")
                        if lon_val is None:
                            lon_val = "28.9784"
                        if row[lon_i] != lon_val:
                            row[lon_i] = lon_val
                            fixed_required += 1

                    if lat_i is not None and not row[lat_i].strip():
                        row[lat_i] = "41.0082"
                        fixed_required += 1
                    if lon_i is not None and not row[lon_i].strip():
                        row[lon_i] = "28.9784"
                        fixed_required += 1

                elif path.name == "trips.txt":
                    missing_trip_required = any(
                        req in idx and not row[idx[req]].strip()
                        for req in ("trip_id", "route_id", "service_id")
                    )
                    if missing_trip_required:
                        dropped += 1
                        continue

                elif path.name == "stop_times.txt":
                    missing_stop_time_required = any(
                        req in idx and not row[idx[req]].strip()
                        for req in ("trip_id", "stop_id", "stop_sequence")
                    )
                    if missing_stop_time_required:
                        dropped += 1
                        continue

                elif path.name == "calendar.txt":
                    service_id_i = idx.get("service_id")
                    if service_id_i is not None and not row[service_id_i].strip():
                        dropped += 1
                        continue

                out.append(row)

            with path.open("w", encoding="utf-8", newline="") as f:
                csv.writer(f).writerows(out)

            report[path.name] = {
                "input_rows": len(rows) - 1,
                "output_rows": len(out) - 1,
                "fixed_width": fixed_width,
                "fixed_required": fixed_required,
                "dropped": dropped,
            }

        zip_path = city_dir / "gtfs.zip"
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
            for p in sorted(gtfs_dir.glob("*.txt")):
                z.write(p, arcname=p.name)

        return report

    def list_cities(self) -> list[dict]:
        """List all configured cities."""
        cities = []
        for city in self.config.all_cities():
            country = self.config.get_country_for_city(city.id)
            cities.append({
                "id": city.id,
                "name": city.name,
                "country": country.id if country else "unknown",
                "sources": [s.type for s in city.sources],
                "otp_port": city.otp.port,
            })
        return cities
