import json
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Set

from . import export
from .http import easyway_request

class GTFSScraper:
    def __init__(
        self,
        cities: List[str],
        output_dir: str = "gtfs_data",
        logs_dir: str = "logs",
    ):
        self.cities = cities
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

        self.logs_dir = Path(logs_dir)
        self.logs_dir.mkdir(parents=True, exist_ok=True)

        # Progress and run logs live under logs_dir, not the GTFS output folder
        self.progress_file = self.logs_dir / "progress.json"
        self.progress = self._load_progress()

        log_file = self.logs_dir / f"scraper_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

        # In-memory structures for GTFS tables
        self.agencies = {}
        self.stops = {}
        self.routes = {}
        self.trips = {}
        self.stop_times = []
        self.calendar = {}
        self.calendar_dates = []
        self.shapes = {}
        self.fare_attributes = {}
        self.fare_rules = []
        self.frequencies = []
        
        # Fare ID mapping (price_currency -> fare_id)
        self.fare_id_map = {}
        
        # Transport type mapping (easyway -> GTFS)
        self.transport_mapping = {
            "bus": 3,           # Bus
            "tram": 0,          # Tram
            "metro": 1,         # Subway/Metro
            "train": 2,         # Rail
            "boat": 4,          # Ferry
            "telecabin": 6,     # Cable car
            "funicular": 7      # Funicular
        }
        
        # Transport type to color mapping (route_color, route_text_color)
        self.transport_colors = {
            0: ("E31837", "FFFFFF"),  # Tram - Red
            1: ("0072BC", "FFFFFF"),  # Metro - Blue
            2: ("6D6E71", "FFFFFF"),  # Rail - Gray
            3: ("00A94F", "FFFFFF"),  # Bus - Green
            4: ("00B5E2", "FFFFFF"),  # Ferry - Light Blue
            5: ("7C4199", "FFFFFF"),  # Cable car - Purple
            6: ("F7931E", "FFFFFF"),  # Aerial lift - Orange
            7: ("ED1C24", "FFFFFF"),  # Funicular - Red
        }
        
        self.headers = {"X-Requested-With": "XMLHttpRequest"}
        self.base_url = "https://tr.easyway.info/ajax/tr"
        
        # Auto-save counter
        self.save_interval = 10  # persist every N operations
        self.operation_count = 0
        
    def _load_progress(self) -> Dict:
        """Load persisted progress state."""
        if self.progress_file.exists():
            with open(self.progress_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}
    
    def _save_progress(self):
        """Persist progress state to disk."""
        with open(self.progress_file, 'w', encoding='utf-8') as f:
            json.dump(self.progress, f, ensure_ascii=False, indent=2)
        self.logger.info("Progress state saved")
    
    def _increment_operation(self):
        """Bump operation counter and flush files when the interval is reached."""
        self.operation_count += 1
        if self.operation_count % self.save_interval == 0:
            self.save_all_files()
            self._save_progress()
    
    def _make_request(self, endpoint: str, params: Dict = None, data: Dict = None, form_data: Dict = None) -> Dict:
        """Perform an HTTP request to the EasyWay API."""
        return easyway_request(
            endpoint,
            headers=self.headers,
            logger=self.logger,
            params=params,
            data=data,
            form_data=form_data,
        )
    
    def scrape_city(self, city: str):
        """Scrape all data for one city."""
        city_lower = city.lower()

        if city_lower in self.progress.get('completed_cities', []):
            self.logger.info(f"{city} already completed, skipping")
            return

        self.logger.info(f"Starting scrape for {city}...")

        self._scrape_agencies(city_lower)

        self._scrape_stops(city_lower)

        self._scrape_routes(city_lower)

        # Mark city as completed
        if 'completed_cities' not in self.progress:
            self.progress['completed_cities'] = []
        self.progress['completed_cities'].append(city_lower)
        self._save_progress()
        
        self.logger.info(f"Finished scrape for {city}")
    
    def _scrape_agencies(self, city: str):
        """Fetch agencies for a city."""
        self.logger.info(f"Fetching agencies for {city}...")
        
        endpoint = f"{self.base_url}/{city}/agencies"
        agencies_data = self._make_request(endpoint)
        
        if not agencies_data:
            return
        
        for agency in agencies_data:
            agency_id = f"{city}_{agency['i']}"
            
            if agency_id not in self.agencies:
                # Fetch detail record
                detail_endpoint = f"{self.base_url}/{city}/agencyInfo/{agency['i']}"
                detail = self._make_request(detail_endpoint)
                
                if detail and 'general' in detail:
                    gen = detail['general']
                    self.agencies[agency_id] = {
                        'agency_id': agency_id,
                        'agency_name': gen['name'],
                        'agency_url': gen.get('url', ''),
                        'agency_timezone': 'Europe/Istanbul',
                        'agency_phone': gen.get('phone', ''),
                        'agency_lang': 'tr'
                    }
                    self.logger.info(f"Agency added: {gen['name']}")
                    
                time.sleep(0.5)  # Rate limiting
        
        self._increment_operation()
    
    def _scrape_stops(self, city: str):
        """Fetch stops for a city."""
        self.logger.info(f"Fetching stops for {city}...")
        
        endpoint = f"{self.base_url}/{city}/stops"
        stops_data = self._make_request(endpoint)
        
        if not stops_data:
            return
        
        for stop_id, stop_info in stops_data.items():
            stop_key = f"{city}_{stop_id}"
            
            if stop_key not in self.stops:
                # Coordinates are scaled (divide by 1e6)
                lat = stop_info[0] / 1000000.0
                lon = stop_info[1] / 1000000.0
                
                self.stops[stop_key] = {
                    'stop_id': stop_key,
                    'stop_code': '',
                    'stop_name': stop_info[2],
                    'stop_desc': '',
                    'stop_lat': lat,
                    'stop_lon': lon,
                    'zone_id': '',
                    'stop_url': '',
                    'location_type': 0,
                    'parent_station': '',
                    'stop_timezone': 'Europe/Istanbul',
                    'wheelchair_boarding': '0',
                    'platform_code': ''
                }
        
        self.logger.info(f"{len(stops_data)} stops added")
        self._increment_operation()
    
    def _scrape_routes(self, city: str):
        """Fetch routes and related schedule/shape data for a city."""
        self.logger.info(f"Fetching routes for {city}...")

        # List routes
        endpoint = f"{self.base_url}/{city}/routes"
        routes_data = self._make_request(endpoint)
        
        if not routes_data or 'routes' not in routes_data:
            return
        
        total_routes = len(routes_data['routes'])
        
        for idx, (route_id, route_info) in enumerate(routes_data['routes'].items(), 1):
            self.logger.info(f"Processing route ({idx}/{total_routes}): {route_info['rn']}")
            
            route_key = f"{city}_{route_id}"
            
            # Skip if this route was already processed
            if route_key in self.progress.get('processed_routes', {}):
                continue
            
            # Add route row
            transport_type = self.transport_mapping.get(route_info['tk'], 3)
            
            agency_id = f"{city}_{route_info.get('sp', '1')}"
            
            # Get default colors for this transport type
            colors = self.transport_colors.get(transport_type, ("FFFFFF", "000000"))
            
            self.routes[route_key] = {
                'route_id': route_key,
                'agency_id': agency_id,
                'route_short_name': route_info['rn'],
                'route_long_name': route_info['rd'],
                'route_desc': route_info['rd'],  # Use long name as description
                'route_type': transport_type,
                'route_url': f"https://tr.easyway.info/tr/{city}/route/{route_id}",
                'route_color': colors[0],
                'route_text_color': colors[1],
                'route_sort_order': ''
            }
            
            # Fare from list payload
            if 'rp' in route_info and route_info['rp']:
                price = route_info['rp']
                currency = route_info.get('cur', 'TRY')
                self._add_fare_info(route_key, price, currency)
            
            # Route detail (shapes, trips, schedules)
            detail_endpoint = f"{self.base_url}/{city}/routeInfo/{route_id}"
            route_detail = self._make_request(detail_endpoint)
            
            if route_detail:
                self._process_route_details(city, route_id, route_key, route_detail)
            
            # Record as processed
            if 'processed_routes' not in self.progress:
                self.progress['processed_routes'] = {}
            self.progress['processed_routes'][route_key] = True
            
            time.sleep(1)  # Rate limiting
            self._increment_operation()
    
    def _process_route_details(self, city: str, route_id: str, route_key: str, detail: Dict):
        """Merge route detail into GTFS structures."""

        # Fare from detail payload
        general = detail.get('general', {})
        if 'pr' in general and general['pr']:
            price = general['pr']
            currency = general.get('cr', 'TRY')
            self._add_fare_info(route_key, price, currency)
        
        # Polylines
        self._process_shapes(city, route_id, route_key, detail.get('scheme', {}))
        
        # Trips and stop_times
        self._process_trips(city, route_id, route_key, detail)
    
    def _parse_day_group(self, day_group: str) -> Set[str]:
        """Parse day group strings like 1-5, 6, 7, or 6,7 into weekday numbers."""
        days = set()
        parts = day_group.split(',')
        for part in parts:
            part = part.strip()
            if ' - ' in part:
                start, end = part.split(' - ')
                start = int(start.strip())
                end = int(end.strip())
                for d in range(start, end + 1):
                    days.add(str(d))
            else:
                days.add(part)
        return days
    
    def _add_calendar(self, city: str, route_id: str, day_group: str) -> str:
        """Insert a calendar row if needed and return service_id."""
        service_id = f"{city}_{route_id}_{day_group.replace(' ', '').replace('-', '_').replace(',', '_')}"
        
        if service_id in self.calendar:
            return service_id
        
        days = self._parse_day_group(day_group)
        start_date = datetime.now().strftime('%Y%m%d')
        end_date = (datetime.now() + timedelta(days=365)).strftime('%Y%m%d')
        
        self.calendar[service_id] = {
            'service_id': service_id,
            'monday': 1 if '1' in days else 0,
            'tuesday': 1 if '2' in days else 0,
            'wednesday': 1 if '3' in days else 0,
            'thursday': 1 if '4' in days else 0,
            'friday': 1 if '5' in days else 0,
            'saturday': 1 if '6' in days else 0,
            'sunday': 1 if '7' in days else 0,
            'start_date': start_date,
            'end_date': end_date
        }
        return service_id
    
    def _process_shapes(self, city: str, route_id: str, route_key: str, scheme: Dict):
        """Parse scheme polylines into shapes."""
        if not scheme:
            return
        
        # Forward shape
        if 'forward' in scheme and scheme['forward']:
            shape_id = f"{route_key}_forward"
            self._add_shape_points(shape_id, scheme['forward'])
        
        # Backward shape
        if 'backward' in scheme and scheme['backward']:
            shape_id = f"{route_key}_backward"
            self._add_shape_points(shape_id, scheme['backward'])
        
        # Secondary shapes
        if 'secondary' in scheme:
            if 'forward' in scheme['secondary']:
                for sec_id, sec_points in scheme['secondary']['forward'].items():
                    if sec_points:
                        shape_id = f"{route_key}_sec_{sec_id}"
                        self._add_shape_points(shape_id, sec_points)
            if 'backward' in scheme['secondary']:
                for sec_id, sec_points in scheme['secondary']['backward'].items():
                    if sec_points:
                        shape_id = f"{route_key}_sec_back_{sec_id}"
                        self._add_shape_points(shape_id, sec_points)
    
    def _add_shape_points(self, shape_id: str, points_str: str):
        """Append shape points from a space-separated lat,lon string."""
        if shape_id in self.shapes:
            return
        
        points = points_str.strip().split()
        shape_points = []
        
        for idx, point in enumerate(points, 1):
            try:
                lat, lon = point.split(',')
                shape_points.append({
                    'shape_id': shape_id,
                    'shape_pt_lat': float(lat),
                    'shape_pt_lon': float(lon),
                    'shape_pt_sequence': idx
                })
            except:
                continue
        
        if shape_points:
            self.shapes[shape_id] = shape_points
    
    def _process_trips(self, city: str, route_id: str, route_key: str, detail: Dict):
        """Build trips and stop_times from schedule endpoints."""
        
        transport_id = detail.get('general', {}).get('ti', '3')
        
        self.logger.info(f"Processing trips for route: {route_key}")

        # Main schedule (lists directions)
        main_schedule = self._get_schedule(city, transport_id, route_id, None)
        
        if not main_schedule or not isinstance(main_schedule, dict):
            self.logger.warning("Main schedule unavailable")
            return
        
        directions = main_schedule.get('directions', {})
        
        if not directions:
            self.logger.warning("No directions in main schedule")
            return
        
        self.logger.info(f"Directions: {list(directions.keys())}")

        # Per-direction schedule
        for direction_key, headsign in directions.items():
            self._process_direction(city, transport_id, route_id, route_key, direction_key, headsign)
    
    def _process_direction(self, city: str, transport_id: str, route_id: str,
                          route_key: str, direction_key: str, headsign: str):
        """Process schedule for one direction key."""

        # Direction-specific schedule
        schedule = self._get_schedule(city, transport_id, route_id, direction_key)
        
        if not schedule or not isinstance(schedule, dict):
            self.logger.warning(f"Schedule unavailable - direction: {direction_key}")
            return
        
        # Stop list for this direction
        stops_list = schedule.get('stops', [])
        
        if not stops_list:
            self.logger.warning(f"No stops - direction: {direction_key}")
            return
        
        self.logger.info(f"Direction '{direction_key}': {len(stops_list)} stops")

        # schedules.schedules holds actual times
        schedules_obj = schedule.get('schedules', {})
        
        if not isinstance(schedules_obj, dict):
            self.logger.warning(f"Invalid schedules object - direction: {direction_key}")
            return
        
        # Inner map: day_group -> payload
        inner_schedules = schedules_obj.get('schedules', {})
        
        if not isinstance(inner_schedules, dict):
            self.logger.warning(f"Invalid inner schedules - direction: {direction_key}")
            return
        
        # Each day group
        for day_group, day_data in inner_schedules.items():
            self._process_day_group(city, route_id, route_key, direction_key, 
                                   headsign, stops_list, day_group, day_data)
    
    def _process_day_group(self, city: str, route_id: str, route_key: str,
                          direction_key: str, headsign: str, stops_list: List,
                          day_group: str, day_data):
        """Create trips for one day_group bucket."""

        # service_id ties calendar to trips
        service_id = self._add_calendar(city, route_id, day_group)
        
        # Empty or list-shaped payload: nothing to expand
        if isinstance(day_data, list):
            self.logger.debug(f"Schedule list found, treating as empty - day_group: {day_group}")
            return
        
        # Frequency style: work_time + interval
        if isinstance(day_data, dict) and 'work_time' in day_data and 'interval' in day_data:
            self._create_frequency_trip(city, route_id, route_key, direction_key,
                                       headsign, stops_list, day_group, 
                                       day_data, service_id)
            return
        
        # Hour buckets with minute lists
        if isinstance(day_data, dict):
            self._create_trips_from_hourly_schedule(city, route_id, route_key, direction_key,
                                                    headsign, stops_list, day_group,
                                                    day_data, service_id)
            return
        
        self.logger.warning(f"Unknown schedule format - day_group: {day_group}, type: {type(day_data)}")
    
    def _create_trips_from_hourly_schedule(self, city: str, route_id: str, route_key: str,
                                          direction_key: str, headsign: str, stops_list: List,
                                          day_group: str, hourly_data: Dict, service_id: str):
        """Expand hour -> minutes dict into discrete trips."""
        
        trip_counter = 0
        
        for hour_key, hour_data in hourly_data.items():
            if not isinstance(hour_data, dict) or 'minutes' not in hour_data:
                continue
            
            minutes_list = hour_data['minutes']
            if not isinstance(minutes_list, list):
                continue
            
            for minute_data in minutes_list:
                if not isinstance(minute_data, dict) or 'min' not in minute_data:
                    continue
                
                trip_counter += 1
                trip_id = f"{route_key}_{direction_key}_{day_group.replace(' ', '_').replace(',', '_')}_{trip_counter}"
                
                # shape_id from direction
                shape_id = self._get_shape_id(route_key, direction_key)
                
                # direction_id: 0 = forward, 1 = backward
                direction_id = '1' if direction_key == 'backward' else '0'
                
                # Trip row
                if trip_id not in self.trips:
                    self.trips[trip_id] = {
                        'route_id': route_key,
                        'service_id': service_id,
                        'trip_id': trip_id,
                        'trip_headsign': headsign,
                        'trip_short_name': '',
                        'direction_id': direction_id,
                        'block_id': '',
                        'shape_id': shape_id,
                        'wheelchair_accessible': '0',
                        'bikes_allowed': '0'
                    }
                
                # stop_times for this trip
                departure_time = f"{hour_key}:{minute_data['min']:02d}:00"
                self._add_stop_times(city, trip_id, stops_list, departure_time)
        
        if trip_counter > 0:
            self.logger.info(f"Direction '{direction_key}', day_group '{day_group}': {trip_counter} trips created")
    
    def _create_frequency_trip(self, city: str, route_id: str, route_key: str,
                              direction_key: str, headsign: str, stops_list: List,
                              day_group: str, freq_data: Dict, service_id: str):
        """Expand headway-based service into synthetic timed trips."""
        
        work_time = freq_data.get('work_time', '')
        interval_str = freq_data.get('interval', '')
        
        if not work_time or not interval_str:
            self.logger.warning(f"Missing work_time or interval - day_group: {day_group}")
            return
        
        try:
            # Parse service window
            start_str, end_str = [t.strip() for t in work_time.split('-')]
            
            # H:M parts
            start_parts = start_str.split(':')
            end_parts = end_str.split(':')
            
            start_hour = int(start_parts[0])
            start_min = int(start_parts[1])
            end_hour = int(end_parts[0])
            end_min = int(end_parts[1])
            
            # Minutes since midnight
            start_total_mins = start_hour * 60 + start_min
            end_total_mins = end_hour * 60 + end_min
            
        except (ValueError, IndexError) as e:
            self.logger.error(f"Invalid work_time: {work_time} - {e}")
            return
        
        try:
            # Headway minutes (average if range)
            if '-' in interval_str:
                min_int, max_int = map(int, [p.strip() for p in interval_str.split('-')])
                interval_mins = (min_int + max_int) / 2
            else:
                interval_mins = int(interval_str)
        except ValueError:
            self.logger.error(f"Invalid interval: {interval_str}")
            return
        
        shape_id = self._get_shape_id(route_key, direction_key)
        
        # direction_id: 0 = forward, 1 = backward
        direction_id = '1' if direction_key == 'backward' else '0'

        # Walk window at headway
        current_mins = start_total_mins
        trip_counter = 0
        
        while current_mins <= end_total_mins:
            trip_counter += 1
            trip_id = f"{route_key}_{direction_key}_{day_group.replace(' ', '_').replace(',', '_')}_{trip_counter}"

            # Trip row
            if trip_id not in self.trips:
                self.trips[trip_id] = {
                    'route_id': route_key,
                    'service_id': service_id,
                    'trip_id': trip_id,
                    'trip_headsign': headsign,
                    'trip_short_name': '',
                    'direction_id': direction_id,
                    'block_id': '',
                    'shape_id': shape_id,
                    'wheelchair_accessible': '0',
                    'bikes_allowed': '0'
                }
            
            # HH:MM:SS departure at first stop
            hours = int(current_mins // 60)
            mins = int(current_mins % 60)
            departure_time = f"{hours:02d}:{mins:02d}:00"

            # stop_times for this trip
            self._add_stop_times(city, trip_id, stops_list, departure_time)
            
            # Next departure
            current_mins += interval_mins
        
        self.logger.info(
            f"Frequency-based {trip_counter} trips - direction: {direction_key}, "
            f"day_group: {day_group}, interval: {interval_mins} min"
        )
    
    def _get_shape_id(self, route_key: str, direction_key: str) -> str:
        """Map API direction keys to shape_id strings."""
        if direction_key == 'forward':
            return f"{route_key}_forward"
        elif direction_key == 'backward':
            return f"{route_key}_backward"
        elif direction_key.startswith('secondary_trip_'):
            sec_id = direction_key.replace('secondary_trip_', '')
            return f"{route_key}_sec_{sec_id}"
        else:
            return f"{route_key}_{direction_key}"
    
    def _get_schedule(self, city: str, transport_id: str, route_id: str, direction_key: str) -> Dict:
        """POST /schedule for optional direction_key."""
        endpoint = f"{self.base_url}/{city}/schedule"
        
        form_data = {
            'transport_id': str(transport_id),
            'route_id': str(route_id),
        }
        
        if direction_key:
            form_data['direction_key'] = direction_key
        
        schedule = self._make_request(endpoint, form_data=form_data)
        time.sleep(0.5)
        return schedule
    
    def _add_stop_times(self, city: str, trip_id: str, stops_list: List, start_time: str):
        """Synthesize stop_times with a fixed dwell/spacing model."""

        # First stop departure
        time_parts = start_time.split(':')
        current_minutes = int(time_parts[0]) * 60 + int(time_parts[1])
        
        # Assume 2 minutes between successive stops
        time_increment = 2
        
        for idx, stop in enumerate(stops_list, 1):
            # Stop id from API ('id' or 'i')
            stop_id_val = stop.get('id') if 'id' in stop else stop.get('i')
            stop_id = f"{city}_{stop_id_val}"
            
            # Cumulative time from start
            minutes = current_minutes + (idx - 1) * time_increment
            hours = minutes // 60
            mins = minutes % 60
            time_str = f"{hours:02d}:{mins:02d}:00"
            
            self.stop_times.append({
                'trip_id': trip_id,
                'arrival_time': time_str,
                'departure_time': time_str,
                'stop_id': stop_id,
                'stop_sequence': idx,
                'stop_headsign': '',
                'pickup_type': '0',  # 0 = regularly scheduled pickup
                'drop_off_type': '0',  # 0 = regularly scheduled drop off
                'shape_dist_traveled': '',
                'timepoint': '1'  # 1 = times are exact
            })
    
    def _add_fare_info(self, route_key: str, price: float, currency: str):
        """Add fare_attributes and fare_rules rows (deduped by price+currency)."""

        # One fare_id per price+currency
        fare_key = f"{price}_{currency}"
        
        if fare_key not in self.fare_id_map:
            # New fare_id
            fare_id = f"fare_{len(self.fare_attributes) + 1}"
            self.fare_id_map[fare_key] = fare_id
            
            # fare_attributes row
            self.fare_attributes[fare_id] = {
                'fare_id': fare_id,
                'price': price,
                'currency_type': currency,
                'payment_method': 0,  # 0 = pay on board
                'transfers': '',  # empty = unlimited per GTFS convention
                'transfer_duration': ''
            }
            
            self.logger.info(f"Fare added: {price} {currency} (ID: {fare_id})")

        # fare_rules row
        fare_id = self.fare_id_map[fare_key]
        self.fare_rules.append({
            'fare_id': fare_id,
            'route_id': route_key,
            'origin_id': '',
            'destination_id': '',
            'contains_id': ''
        })
    
    def save_all_files(self):
        """Write all GTFS tables to CSV files in output_dir."""
        export.save_all_files(self)

    def run(self):
        """Scrape all configured cities and flush outputs."""
        self.logger.info("Starting GTFS scrape...")
        self.logger.info(f"Cities: {', '.join(self.cities)}")
        
        try:
            for city in self.cities:
                self.scrape_city(city)
            
            self.save_all_files()
            self._save_progress()

            self.logger.info("All done.")

        except Exception as e:
            self.logger.error(f"Fatal error: {str(e)}", exc_info=True)
            # Best-effort save on failure
            self.save_all_files()
            self._save_progress()