"""Gateway configuration."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass
class CityOTP:
    """City OTP configuration."""
    
    id: str
    name: str
    bbox: tuple[float, float, float, float]  # min_lon, min_lat, max_lon, max_lat
    port: int
    memory: str
    graph_path: Path
    
    def contains_point(self, lat: float, lon: float) -> bool:
        """Check if coordinates are within this city's bounding box."""
        min_lon, min_lat, max_lon, max_lat = self.bbox
        return min_lon <= lon <= max_lon and min_lat <= lat <= max_lat


@dataclass
class GatewayConfig:
    """Gateway configuration."""
    
    cities: dict[str, CityOTP]
    otp_image: str = "opentripplanner/opentripplanner:2.5.0"
    container_idle_timeout: int = 300  # seconds
    container_startup_timeout: int = 300  # seconds (5 min for large graphs)
    health_check_interval: int = 5  # seconds
    
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
                )
        
        return cls(cities=cities)
    
    def find_city_by_coordinates(self, lat: float, lon: float) -> CityOTP | None:
        """Find city containing the given coordinates."""
        for city in self.cities.values():
            if city.contains_point(lat, lon):
                return city
        return None
    
    def get_city(self, city_id: str) -> CityOTP | None:
        """Get city by ID."""
        return self.cities.get(city_id)
