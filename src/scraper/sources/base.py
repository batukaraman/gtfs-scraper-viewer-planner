"""Abstract base class for GTFS data sources."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


@dataclass
class GTFSData:
    """Container for GTFS data tables."""

    agencies: dict[str, dict[str, Any]] = field(default_factory=dict)
    stops: dict[str, dict[str, Any]] = field(default_factory=dict)
    routes: dict[str, dict[str, Any]] = field(default_factory=dict)
    trips: dict[str, dict[str, Any]] = field(default_factory=dict)
    stop_times: list[dict[str, Any]] = field(default_factory=list)
    calendar: dict[str, dict[str, Any]] = field(default_factory=dict)
    calendar_dates: list[dict[str, Any]] = field(default_factory=list)
    shapes: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    fare_attributes: dict[str, dict[str, Any]] = field(default_factory=dict)
    fare_rules: list[dict[str, Any]] = field(default_factory=list)
    frequencies: list[dict[str, Any]] = field(default_factory=list)

    def merge(self, other: GTFSData) -> None:
        """Merge another GTFSData into this one."""
        self.agencies.update(other.agencies)
        self.stops.update(other.stops)
        self.routes.update(other.routes)
        self.trips.update(other.trips)
        self.stop_times.extend(other.stop_times)
        self.calendar.update(other.calendar)
        self.calendar_dates.extend(other.calendar_dates)
        for shape_id, points in other.shapes.items():
            if shape_id not in self.shapes:
                self.shapes[shape_id] = []
            self.shapes[shape_id].extend(points)
        self.fare_attributes.update(other.fare_attributes)
        self.fare_rules.extend(other.fare_rules)
        self.frequencies.extend(other.frequencies)

    def stats(self) -> dict[str, int]:
        """Return counts for each table."""
        shape_points = sum(len(pts) for pts in self.shapes.values())
        return {
            "agencies": len(self.agencies),
            "stops": len(self.stops),
            "routes": len(self.routes),
            "trips": len(self.trips),
            "stop_times": len(self.stop_times),
            "calendar": len(self.calendar),
            "calendar_dates": len(self.calendar_dates),
            "shapes": len(self.shapes),
            "shape_points": shape_points,
            "fare_attributes": len(self.fare_attributes),
            "fare_rules": len(self.fare_rules),
            "frequencies": len(self.frequencies),
        }


class GTFSSource(ABC):
    """Abstract base class for GTFS data sources.
    
    Subclasses implement scraping from specific APIs or downloading static feeds.
    """

    @property
    @abstractmethod
    def source_type(self) -> str:
        """Return the source type identifier (e.g., 'easyway', 'static_gtfs')."""
        pass

    @abstractmethod
    def scrape(self) -> GTFSData:
        """Scrape/download GTFS data and return it.
        
        Returns:
            GTFSData: Container with all GTFS tables populated.
        """
        pass

    @abstractmethod
    def supports_resume(self) -> bool:
        """Whether this source supports resuming interrupted scrapes."""
        pass
