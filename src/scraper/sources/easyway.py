"""EasyWay API GTFS scraper."""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

from ..config import CityConfig, SourceConfig
from .base import GTFSData, GTFSSource

logger = logging.getLogger(__name__)


class EasyWaySource(GTFSSource):
    """Scrape GTFS data from EasyWay API."""

    TRANSPORT_MAPPING = {
        "bus": 3,
        "tram": 0,
        "metro": 1,
        "train": 2,
        "boat": 4,
        "telecabin": 6,
        "funicular": 7,
    }

    TRANSPORT_COLORS = {
        0: ("E31837", "FFFFFF"),  # Tram - Red
        1: ("0072BC", "FFFFFF"),  # Metro - Blue
        2: ("6D6E71", "FFFFFF"),  # Rail - Gray
        3: ("00A94F", "FFFFFF"),  # Bus - Green
        4: ("00B5E2", "FFFFFF"),  # Ferry - Light Blue
        5: ("7C4199", "FFFFFF"),  # Cable car - Purple
        6: ("F7931E", "FFFFFF"),  # Aerial lift - Orange
        7: ("ED1C24", "FFFFFF"),  # Funicular - Red
    }

    def __init__(
        self,
        city_config: CityConfig,
        source_config: SourceConfig,
        progress_dir: Path | None = None,
    ):
        self.city = city_config
        self.source = source_config
        self.city_code = source_config.city_code
        self.base_url = source_config.base_url
        
        self.progress_dir = progress_dir or Path("logs")
        self.progress_dir.mkdir(parents=True, exist_ok=True)
        self.progress_file = self.progress_dir / f"progress_{self.city_code}.json"
        
        self.headers = {"X-Requested-With": "XMLHttpRequest"}
        self.data = GTFSData()
        self.fare_id_map: dict[str, str] = {}
        self.progress: dict[str, Any] = self._load_progress()
        
        self.save_interval = 10
        self.operation_count = 0

    @property
    def source_type(self) -> str:
        return "easyway"

    def supports_resume(self) -> bool:
        return True

    def _load_progress(self) -> dict:
        if self.progress_file.exists():
            with open(self.progress_file, encoding="utf-8") as f:
                return json.load(f)
        return {}

    def _save_progress(self) -> None:
        with open(self.progress_file, "w", encoding="utf-8") as f:
            json.dump(self.progress, f, ensure_ascii=False, indent=2)

    def _request(
        self,
        endpoint: str,
        params: dict | None = None,
        form_data: dict | None = None,
        timeout: int = 30,
    ) -> Any:
        """Make HTTP request to EasyWay API."""
        try:
            if form_data:
                response = requests.post(
                    endpoint, headers=self.headers, data=form_data, timeout=timeout
                )
            else:
                response = requests.get(
                    endpoint, headers=self.headers, params=params, timeout=timeout
                )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error("Request failed (%s): %s", endpoint, e)
            return None

    def _increment_operation(self) -> None:
        self.operation_count += 1
        if self.operation_count % self.save_interval == 0:
            self._save_progress()

    def scrape(self) -> GTFSData:
        """Scrape all GTFS data from EasyWay API."""
        logger.info("Starting EasyWay scrape for %s...", self.city.name)
        
        self._scrape_agencies()
        self._scrape_stops()
        self._scrape_routes()
        
        self._save_progress()
        logger.info("EasyWay scrape completed for %s", self.city.name)
        
        return self.data

    def _scrape_agencies(self) -> None:
        """Fetch agencies for the city."""
        logger.info("Fetching agencies...")
        
        endpoint = f"{self.base_url}/{self.city_code}/agencies"
        agencies_data = self._request(endpoint)
        
        if not agencies_data:
            return
        
        for agency in agencies_data:
            agency_id = f"{self.city_code}_{agency['i']}"
            
            if agency_id in self.data.agencies:
                continue
            
            detail_endpoint = f"{self.base_url}/{self.city_code}/agencyInfo/{agency['i']}"
            detail = self._request(detail_endpoint)
            
            if detail and "general" in detail:
                gen = detail["general"]
                self.data.agencies[agency_id] = {
                    "agency_id": agency_id,
                    "agency_name": gen["name"],
                    "agency_url": gen.get("url", ""),
                    "agency_timezone": self.city.timezone,
                    "agency_phone": gen.get("phone", ""),
                    "agency_lang": self.city.language,
                }
                logger.debug("Agency added: %s", gen["name"])
            
            time.sleep(0.5)
        
        self._increment_operation()

    def _scrape_stops(self) -> None:
        """Fetch stops for the city."""
        logger.info("Fetching stops...")
        
        endpoint = f"{self.base_url}/{self.city_code}/stops"
        stops_data = self._request(endpoint)
        
        if not stops_data:
            return
        
        for stop_id, stop_info in stops_data.items():
            stop_key = f"{self.city_code}_{stop_id}"
            
            if stop_key in self.data.stops:
                continue
            
            lat = stop_info[0] / 1_000_000.0
            lon = stop_info[1] / 1_000_000.0
            
            self.data.stops[stop_key] = {
                "stop_id": stop_key,
                "stop_code": "",
                "stop_name": stop_info[2],
                "stop_desc": "",
                "stop_lat": lat,
                "stop_lon": lon,
                "zone_id": "",
                "stop_url": "",
                "location_type": 0,
                "parent_station": "",
                "stop_timezone": self.city.timezone,
                "wheelchair_boarding": "0",
                "platform_code": "",
            }
        
        logger.info("%d stops added", len(stops_data))
        self._increment_operation()

    def _scrape_routes(self) -> None:
        """Fetch routes and schedules."""
        logger.info("Fetching routes...")
        
        endpoint = f"{self.base_url}/{self.city_code}/routes"
        routes_data = self._request(endpoint)
        
        if not routes_data or "routes" not in routes_data:
            return
        
        total_routes = len(routes_data["routes"])
        
        for idx, (route_id, route_info) in enumerate(routes_data["routes"].items(), 1):
            logger.info("Processing route (%d/%d): %s", idx, total_routes, route_info["rn"])
            
            route_key = f"{self.city_code}_{route_id}"
            
            if route_key in self.progress.get("processed_routes", {}):
                continue
            
            transport_type = self.TRANSPORT_MAPPING.get(route_info["tk"], 3)
            agency_id = f"{self.city_code}_{route_info.get('sp', '1')}"
            colors = self.TRANSPORT_COLORS.get(transport_type, ("FFFFFF", "000000"))
            
            self.data.routes[route_key] = {
                "route_id": route_key,
                "agency_id": agency_id,
                "route_short_name": route_info["rn"],
                "route_long_name": route_info["rd"],
                "route_desc": route_info["rd"],
                "route_type": transport_type,
                "route_url": f"https://tr.easyway.info/tr/{self.city_code}/route/{route_id}",
                "route_color": colors[0],
                "route_text_color": colors[1],
                "route_sort_order": "",
            }
            
            if "rp" in route_info and route_info["rp"]:
                self._add_fare_info(route_key, route_info["rp"], route_info.get("cur", "TRY"))
            
            detail_endpoint = f"{self.base_url}/{self.city_code}/routeInfo/{route_id}"
            route_detail = self._request(detail_endpoint)
            
            if route_detail:
                self._process_route_details(route_id, route_key, route_detail)
            
            if "processed_routes" not in self.progress:
                self.progress["processed_routes"] = {}
            self.progress["processed_routes"][route_key] = True
            
            time.sleep(1)
            self._increment_operation()

    def _process_route_details(self, route_id: str, route_key: str, detail: dict) -> None:
        """Process route detail including shapes and trips."""
        general = detail.get("general", {})
        if "pr" in general and general["pr"]:
            self._add_fare_info(route_key, general["pr"], general.get("cr", "TRY"))
        
        self._process_shapes(route_key, detail.get("scheme", {}))
        self._process_trips(route_id, route_key, detail)

    def _process_shapes(self, route_key: str, scheme: dict) -> None:
        """Parse scheme polylines into shapes."""
        if not scheme:
            return
        
        if "forward" in scheme and scheme["forward"]:
            self._add_shape_points(f"{route_key}_forward", scheme["forward"])
        
        if "backward" in scheme and scheme["backward"]:
            self._add_shape_points(f"{route_key}_backward", scheme["backward"])
        
        if "secondary" in scheme:
            for direction in ("forward", "backward"):
                if direction in scheme["secondary"]:
                    for sec_id, sec_points in scheme["secondary"][direction].items():
                        if sec_points:
                            suffix = f"_sec_{sec_id}" if direction == "forward" else f"_sec_back_{sec_id}"
                            self._add_shape_points(f"{route_key}{suffix}", sec_points)

    def _add_shape_points(self, shape_id: str, points_str: str) -> None:
        """Add shape points from space-separated lat,lon string."""
        if shape_id in self.data.shapes:
            return
        
        points = points_str.strip().split()
        shape_points = []
        
        for idx, point in enumerate(points, 1):
            try:
                lat, lon = point.split(",")
                shape_points.append({
                    "shape_id": shape_id,
                    "shape_pt_lat": float(lat),
                    "shape_pt_lon": float(lon),
                    "shape_pt_sequence": idx,
                })
            except (ValueError, IndexError):
                continue
        
        if shape_points:
            self.data.shapes[shape_id] = shape_points

    def _process_trips(self, route_id: str, route_key: str, detail: dict) -> None:
        """Build trips and stop_times from schedule."""
        transport_id = detail.get("general", {}).get("ti", "3")
        
        main_schedule = self._get_schedule(transport_id, route_id, None)
        
        if not main_schedule or not isinstance(main_schedule, dict):
            return
        
        directions = main_schedule.get("directions", {})
        
        if not directions:
            return
        
        for direction_key, headsign in directions.items():
            self._process_direction(transport_id, route_id, route_key, direction_key, headsign)

    def _process_direction(
        self,
        transport_id: str,
        route_id: str,
        route_key: str,
        direction_key: str,
        headsign: str,
    ) -> None:
        """Process schedule for one direction."""
        schedule = self._get_schedule(transport_id, route_id, direction_key)
        
        if not schedule or not isinstance(schedule, dict):
            return
        
        stops_list = schedule.get("stops", [])
        
        if not stops_list:
            return
        
        schedules_obj = schedule.get("schedules", {})
        
        if not isinstance(schedules_obj, dict):
            return
        
        inner_schedules = schedules_obj.get("schedules", {})
        
        if not isinstance(inner_schedules, dict):
            return
        
        for day_group, day_data in inner_schedules.items():
            self._process_day_group(
                route_id, route_key, direction_key, headsign, stops_list, day_group, day_data
            )

    def _process_day_group(
        self,
        route_id: str,
        route_key: str,
        direction_key: str,
        headsign: str,
        stops_list: list,
        day_group: str,
        day_data: Any,
    ) -> None:
        """Create trips for one day_group."""
        service_id = self._add_calendar(route_id, day_group)
        
        if isinstance(day_data, list):
            return
        
        if isinstance(day_data, dict) and "work_time" in day_data and "interval" in day_data:
            self._create_frequency_trips(
                route_id, route_key, direction_key, headsign, stops_list, day_group, day_data, service_id
            )
            return
        
        if isinstance(day_data, dict):
            self._create_trips_from_hourly(
                route_id, route_key, direction_key, headsign, stops_list, day_group, day_data, service_id
            )

    def _create_trips_from_hourly(
        self,
        route_id: str,
        route_key: str,
        direction_key: str,
        headsign: str,
        stops_list: list,
        day_group: str,
        hourly_data: dict,
        service_id: str,
    ) -> None:
        """Expand hour -> minutes dict into discrete trips."""
        trip_counter = 0
        
        for hour_key, hour_data in hourly_data.items():
            if not isinstance(hour_data, dict) or "minutes" not in hour_data:
                continue
            
            minutes_list = hour_data["minutes"]
            if not isinstance(minutes_list, list):
                continue
            
            for minute_data in minutes_list:
                if not isinstance(minute_data, dict) or "min" not in minute_data:
                    continue
                
                trip_counter += 1
                trip_id = f"{route_key}_{direction_key}_{day_group.replace(' ', '_').replace(',', '_')}_{trip_counter}"
                
                shape_id = self._get_shape_id(route_key, direction_key)
                direction_id = "1" if direction_key == "backward" else "0"
                
                if trip_id not in self.data.trips:
                    self.data.trips[trip_id] = {
                        "route_id": route_key,
                        "service_id": service_id,
                        "trip_id": trip_id,
                        "trip_headsign": headsign,
                        "trip_short_name": "",
                        "direction_id": direction_id,
                        "block_id": "",
                        "shape_id": shape_id,
                        "wheelchair_accessible": "0",
                        "bikes_allowed": "0",
                    }
                
                departure_time = f"{hour_key}:{minute_data['min']:02d}:00"
                self._add_stop_times(trip_id, stops_list, departure_time)

    def _create_frequency_trips(
        self,
        route_id: str,
        route_key: str,
        direction_key: str,
        headsign: str,
        stops_list: list,
        day_group: str,
        freq_data: dict,
        service_id: str,
    ) -> None:
        """Expand headway-based service into synthetic trips."""
        work_time = freq_data.get("work_time", "")
        interval_str = freq_data.get("interval", "")
        
        if not work_time or not interval_str:
            return
        
        try:
            start_str, end_str = [t.strip() for t in work_time.split("-")]
            start_parts = start_str.split(":")
            end_parts = end_str.split(":")
            
            start_total_mins = int(start_parts[0]) * 60 + int(start_parts[1])
            end_total_mins = int(end_parts[0]) * 60 + int(end_parts[1])
        except (ValueError, IndexError):
            return
        
        try:
            if "-" in interval_str:
                min_int, max_int = map(int, [p.strip() for p in interval_str.split("-")])
                interval_mins = (min_int + max_int) / 2
            else:
                interval_mins = int(interval_str)
        except ValueError:
            return
        
        shape_id = self._get_shape_id(route_key, direction_key)
        direction_id = "1" if direction_key == "backward" else "0"
        
        current_mins = start_total_mins
        trip_counter = 0
        
        while current_mins <= end_total_mins:
            trip_counter += 1
            trip_id = f"{route_key}_{direction_key}_{day_group.replace(' ', '_').replace(',', '_')}_{trip_counter}"
            
            if trip_id not in self.data.trips:
                self.data.trips[trip_id] = {
                    "route_id": route_key,
                    "service_id": service_id,
                    "trip_id": trip_id,
                    "trip_headsign": headsign,
                    "trip_short_name": "",
                    "direction_id": direction_id,
                    "block_id": "",
                    "shape_id": shape_id,
                    "wheelchair_accessible": "0",
                    "bikes_allowed": "0",
                }
            
            hours = int(current_mins // 60)
            mins = int(current_mins % 60)
            departure_time = f"{hours:02d}:{mins:02d}:00"
            
            self._add_stop_times(trip_id, stops_list, departure_time)
            current_mins += interval_mins

    def _get_schedule(self, transport_id: str, route_id: str, direction_key: str | None) -> dict | None:
        """Get schedule from API."""
        endpoint = f"{self.base_url}/{self.city_code}/schedule"
        
        form_data = {
            "transport_id": str(transport_id),
            "route_id": str(route_id),
        }
        
        if direction_key:
            form_data["direction_key"] = direction_key
        
        schedule = self._request(endpoint, form_data=form_data)
        time.sleep(0.5)
        return schedule

    def _add_stop_times(self, trip_id: str, stops_list: list, start_time: str) -> None:
        """Create stop_times for a trip."""
        time_parts = start_time.split(":")
        current_minutes = int(time_parts[0]) * 60 + int(time_parts[1])
        time_increment = 2
        
        seq = 0
        for stop in stops_list:
            stop_id_val = stop.get("id") if "id" in stop else stop.get("i")
            if stop_id_val is None or stop_id_val == "":
                continue
            
            seq += 1
            stop_id = f"{self.city_code}_{stop_id_val}"
            
            self._ensure_stop_exists(stop_id, stop)
            
            minutes = current_minutes + (seq - 1) * time_increment
            hours = minutes // 60
            mins = minutes % 60
            time_str = f"{hours:02d}:{mins:02d}:00"
            
            self.data.stop_times.append({
                "trip_id": trip_id,
                "arrival_time": time_str,
                "departure_time": time_str,
                "stop_id": stop_id,
                "stop_sequence": seq,
                "stop_headsign": "",
                "pickup_type": "0",
                "drop_off_type": "0",
                "shape_dist_traveled": "",
                "timepoint": "1",
            })

    def _ensure_stop_exists(self, stop_id: str, stop_data: dict) -> None:
        """Ensure stop exists in data, create placeholder if needed."""
        if stop_id in self.data.stops:
            return
        
        name = (
            stop_data.get("n")
            or stop_data.get("name")
            or stop_data.get("nm")
            or stop_data.get("t")
            or "Durak"
        )
        
        lat, lon = self.city.bbox.min_lat, self.city.bbox.min_lon
        
        if "lat" in stop_data and "lon" in stop_data:
            try:
                lat = float(stop_data["lat"])
                lon = float(stop_data["lon"])
            except (TypeError, ValueError):
                pass
        elif "la" in stop_data and "lo" in stop_data:
            try:
                lat = float(stop_data["la"])
                lon = float(stop_data["lo"])
            except (TypeError, ValueError):
                pass
        elif isinstance(stop_data.get("c"), (list, tuple)) and len(stop_data["c"]) >= 2:
            try:
                lat = float(stop_data["c"][0]) / 1_000_000.0
                lon = float(stop_data["c"][1]) / 1_000_000.0
            except (TypeError, ValueError, IndexError):
                pass
        
        self.data.stops[stop_id] = {
            "stop_id": stop_id,
            "stop_code": "",
            "stop_name": str(name)[:256],
            "stop_desc": "",
            "stop_lat": lat,
            "stop_lon": lon,
            "zone_id": "",
            "stop_url": "",
            "location_type": 0,
            "parent_station": "",
            "stop_timezone": self.city.timezone,
            "wheelchair_boarding": "0",
            "platform_code": "",
        }

    def _get_shape_id(self, route_key: str, direction_key: str) -> str:
        """Map direction key to shape_id."""
        if direction_key == "forward":
            return f"{route_key}_forward"
        elif direction_key == "backward":
            return f"{route_key}_backward"
        elif direction_key.startswith("secondary_trip_"):
            sec_id = direction_key.replace("secondary_trip_", "")
            return f"{route_key}_sec_{sec_id}"
        else:
            return f"{route_key}_{direction_key}"

    def _parse_day_group(self, day_group: str) -> set[str]:
        """Parse day group strings like 1-5, 6, 7."""
        days: set[str] = set()
        parts = day_group.split(",")
        for part in parts:
            part = part.strip()
            if " - " in part:
                start, end = part.split(" - ")
                for d in range(int(start.strip()), int(end.strip()) + 1):
                    days.add(str(d))
            else:
                days.add(part)
        return days

    def _add_calendar(self, route_id: str, day_group: str) -> str:
        """Add calendar entry and return service_id."""
        service_id = f"{self.city_code}_{route_id}_{day_group.replace(' ', '').replace('-', '_').replace(',', '_')}"
        
        if service_id in self.data.calendar:
            return service_id
        
        days = self._parse_day_group(day_group)
        start_date = datetime.now().strftime("%Y%m%d")
        end_date = (datetime.now() + timedelta(days=365)).strftime("%Y%m%d")
        
        self.data.calendar[service_id] = {
            "service_id": service_id,
            "monday": 1 if "1" in days else 0,
            "tuesday": 1 if "2" in days else 0,
            "wednesday": 1 if "3" in days else 0,
            "thursday": 1 if "4" in days else 0,
            "friday": 1 if "5" in days else 0,
            "saturday": 1 if "6" in days else 0,
            "sunday": 1 if "7" in days else 0,
            "start_date": start_date,
            "end_date": end_date,
        }
        return service_id

    def _add_fare_info(self, route_key: str, price: float, currency: str) -> None:
        """Add fare info for a route."""
        fare_key = f"{price}_{currency}"
        
        if fare_key not in self.fare_id_map:
            fare_id = f"fare_{len(self.data.fare_attributes) + 1}"
            self.fare_id_map[fare_key] = fare_id
            
            self.data.fare_attributes[fare_id] = {
                "fare_id": fare_id,
                "price": price,
                "currency_type": currency,
                "payment_method": 0,
                "transfers": "",
                "transfer_duration": "",
            }
        
        fare_id = self.fare_id_map[fare_key]
        self.data.fare_rules.append({
            "fare_id": fare_id,
            "route_id": route_key,
            "origin_id": "",
            "destination_id": "",
            "contains_id": "",
        })
