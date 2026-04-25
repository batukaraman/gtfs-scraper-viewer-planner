"""Gateway configuration."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Literal

import yaml

CityTier = Literal["hot", "warm", "cold"]


@dataclass
class CityOTP:
    """City OTP configuration."""
    
    id: str
    name: str
    bbox: tuple[float, float, float, float]  # min_lon, min_lat, max_lon, max_lat
    port: int
    memory: str
    graph_path: Path
    tier: CityTier = "cold"
    idle_timeout: int | None = None
    prewarm_windows: list[tuple[int, int]] | None = None
    
    def contains_point(self, lat: float, lon: float) -> bool:
        """Check if coordinates are within this city's bounding box."""
        min_lon, min_lat, max_lon, max_lat = self.bbox
        return min_lon <= lon <= max_lon and min_lat <= lat <= max_lat

    def should_prewarm_now(self, now: datetime | None = None) -> bool:
        """Check whether city should be pre-warmed for the given local time."""
        if self.tier == "hot":
            return True
        if self.tier != "warm":
            return False
        if not self.prewarm_windows:
            return False

        now = now or datetime.now()
        minute_of_day = now.hour * 60 + now.minute
        for start_min, end_min in self.prewarm_windows:
            if start_min <= end_min:
                if start_min <= minute_of_day <= end_min:
                    return True
            else:
                # Handles overnight windows like 23:00-02:00.
                if minute_of_day >= start_min or minute_of_day <= end_min:
                    return True
        return False


@dataclass
class GatewayConfig:
    """Gateway configuration."""
    
    cities: dict[str, CityOTP]
    otp_image: str = "opentripplanner/opentripplanner:2.5.0"
    container_idle_timeout: int = 300  # seconds
    warm_city_idle_timeout: int = 1800  # seconds
    container_startup_timeout: int = 300  # seconds (5 min for large graphs)
    health_check_interval: int = 5  # seconds
    prewarm_poll_interval: int = 60  # seconds
    plan_cache_ttl_seconds: int = 90
    
    @classmethod
    def load(cls, config_path: Path, data_dir: Path) -> GatewayConfig:
        """Load configuration from cities.yaml.
        
        Args:
            config_path: Path to cities.yaml
            data_dir: Base data directory (graph at data_dir/{city}/graph.obj)
        """
        with open(config_path, encoding="utf-8") as f:
            raw = yaml.safe_load(f)
        
        cities = {}
        port_counter = 8080
        
        for country_id, country_data in raw.get("countries", {}).items():
            for city_id, city_data in country_data.get("cities", {}).items():
                bbox_data = city_data.get("bbox", {})
                otp_data = city_data.get("otp", {})
                
                port = otp_data.get("port", port_counter)
                port_counter = max(port_counter, port) + 1
                
                # Graph is now at data/{city}/graph.obj
                graph_path = data_dir / city_id
                tier = str(otp_data.get("tier", "cold")).lower()
                if tier not in {"hot", "warm", "cold"}:
                    tier = "cold"
                
                cities[city_id] = CityOTP(
                    id=city_id,
                    name=city_data.get("name", city_id),
                    bbox=(
                        bbox_data.get("min_lon", 0),
                        bbox_data.get("min_lat", 0),
                        bbox_data.get("max_lon", 0),
                        bbox_data.get("max_lat", 0),
                    ),
                    port=port,
                    memory=otp_data.get("memory", "4g"),
                    graph_path=graph_path,
                    tier=tier,  # type: ignore[arg-type]
                    idle_timeout=otp_data.get("idle_timeout"),
                    prewarm_windows=_parse_prewarm_windows(otp_data.get("prewarm_windows", [])),
                )
        
        gateway_data = raw.get("gateway", {})
        return cls(
            cities=cities,
            container_idle_timeout=gateway_data.get("container_idle_timeout", 300),
            warm_city_idle_timeout=gateway_data.get("warm_city_idle_timeout", 1800),
            container_startup_timeout=gateway_data.get("container_startup_timeout", 300),
            health_check_interval=gateway_data.get("health_check_interval", 5),
            prewarm_poll_interval=gateway_data.get("prewarm_poll_interval", 60),
            plan_cache_ttl_seconds=gateway_data.get("plan_cache_ttl_seconds", 90),
        )
    
    def find_city_by_coordinates(self, lat: float, lon: float) -> CityOTP | None:
        """Find city containing the given coordinates."""
        for city in self.cities.values():
            if city.contains_point(lat, lon):
                return city
        return None
    
    def get_city(self, city_id: str) -> CityOTP | None:
        """Get city by ID."""
        return self.cities.get(city_id)


def _parse_prewarm_windows(raw_windows: list[str]) -> list[tuple[int, int]]:
    """Parse ['07:00-10:00', '17:00-20:00'] into minute ranges."""
    parsed: list[tuple[int, int]] = []
    for item in raw_windows:
        if not isinstance(item, str) or "-" not in item:
            continue
        left, right = item.split("-", 1)
        start = _parse_hhmm(left.strip())
        end = _parse_hhmm(right.strip())
        if start is None or end is None:
            continue
        parsed.append((start, end))
    return parsed


def _parse_hhmm(value: str) -> int | None:
    parts = value.split(":")
    if len(parts) != 2:
        return None
    try:
        hour = int(parts[0])
        minute = int(parts[1])
    except ValueError:
        return None
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        return None
    return hour * 60 + minute
