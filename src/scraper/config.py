"""Configuration loader for cities and sources."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass
class BoundingBox:
    """Geographic bounding box."""

    min_lon: float
    min_lat: float
    max_lon: float
    max_lat: float

    def to_osmium_string(self) -> str:
        """Format for osmium extract: min_lon,min_lat,max_lon,max_lat"""
        return f"{self.min_lon},{self.min_lat},{self.max_lon},{self.max_lat}"


@dataclass
class SourceConfig:
    """GTFS data source configuration."""

    type: str
    city_code: str = ""
    base_url: str = ""
    url: str = ""
    extra: dict = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict) -> SourceConfig:
        return cls(
            type=data.get("type", ""),
            city_code=data.get("city_code", ""),
            base_url=data.get("base_url", ""),
            url=data.get("url", ""),
            extra={k: v for k, v in data.items() if k not in ("type", "city_code", "base_url", "url")},
        )


@dataclass
class OTPConfig:
    """OTP instance configuration."""

    memory: str = "4g"
    port: int = 8080


@dataclass
class CityConfig:
    """City configuration."""

    id: str
    name: str
    country_id: str
    timezone: str
    language: str
    bbox: BoundingBox
    sources: list[SourceConfig]
    otp: OTPConfig

    @classmethod
    def from_dict(cls, city_id: str, country_id: str, data: dict) -> CityConfig:
        bbox_data = data.get("bbox", {})
        otp_data = data.get("otp", {})
        sources_data = data.get("sources", [])

        return cls(
            id=city_id,
            name=data.get("name", city_id),
            country_id=country_id,
            timezone=data.get("timezone", "UTC"),
            language=data.get("language", "en"),
            bbox=BoundingBox(
                min_lon=bbox_data.get("min_lon", 0),
                min_lat=bbox_data.get("min_lat", 0),
                max_lon=bbox_data.get("max_lon", 0),
                max_lat=bbox_data.get("max_lat", 0),
            ),
            sources=[SourceConfig.from_dict(s) for s in sources_data],
            otp=OTPConfig(
                memory=otp_data.get("memory", "4g"),
                port=otp_data.get("port", 8080),
            ),
        )


@dataclass
class OSMConfig:
    """Country-level OSM configuration."""

    source: str
    filename: str


@dataclass
class CountryConfig:
    """Country configuration."""

    id: str
    name: str
    osm: OSMConfig
    cities: dict[str, CityConfig]

    @classmethod
    def from_dict(cls, country_id: str, data: dict) -> CountryConfig:
        osm_data = data.get("osm", {})
        cities_data = data.get("cities", {})

        cities = {
            city_id: CityConfig.from_dict(city_id, country_id, city_data)
            for city_id, city_data in cities_data.items()
        }

        return cls(
            id=country_id,
            name=data.get("name", country_id),
            osm=OSMConfig(
                source=osm_data.get("source", ""),
                filename=osm_data.get("filename", ""),
            ),
            cities=cities,
        )


@dataclass
class PipelineConfig:
    """Complete pipeline configuration."""

    countries: dict[str, CountryConfig]

    @classmethod
    def load(cls, config_path: Path | str) -> PipelineConfig:
        """Load configuration from YAML file."""
        config_path = Path(config_path)
        
        with open(config_path, encoding="utf-8") as f:
            raw = yaml.safe_load(f)

        countries_data = raw.get("countries", {})
        countries = {
            country_id: CountryConfig.from_dict(country_id, country_data)
            for country_id, country_data in countries_data.items()
        }

        return cls(countries=countries)

    def get_city(self, city_id: str) -> CityConfig | None:
        """Find city by ID across all countries."""
        for country in self.countries.values():
            if city_id in country.cities:
                return country.cities[city_id]
        return None

    def get_country_for_city(self, city_id: str) -> CountryConfig | None:
        """Find country containing the city."""
        for country in self.countries.values():
            if city_id in country.cities:
                return country
        return None

    def all_cities(self) -> list[CityConfig]:
        """Get all cities from all countries."""
        cities = []
        for country in self.countries.values():
            cities.extend(country.cities.values())
        return cities


def get_default_config_path() -> Path:
    """Get default config file path."""
    return Path(__file__).parent.parent.parent / "config" / "cities.yaml"


def load_config(config_path: Path | str | None = None) -> PipelineConfig:
    """Load configuration from file or default location."""
    if config_path is None:
        config_path = get_default_config_path()
    return PipelineConfig.load(config_path)
