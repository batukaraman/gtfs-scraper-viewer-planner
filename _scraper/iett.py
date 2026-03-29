"""
Build an IETT-oriented GTFS extract: main line patterns plus express (depar) variants.
"""

import requests
import json
import csv
import time
from bs4 import BeautifulSoup
from typing import List, Dict
import pandas as pd
from math import radians, sin, cos, sqrt, atan2
import os
import re

# ==================== CONFIG ====================
TEST_MODE = False
RATE_LIMIT_DELAY = 0.5
OUTPUT_DIR = "gtfs_output"

EXISTING_ROUTES_CSV = "gtfs/routes.csv"
EXISTING_STOPS_CSV = "gtfs/stops.csv"
GEOJSON_FILE = "iett-hat-guzergahlar.geojson"

# ==================== HELPERS ====================

def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two WGS84 points in meters."""
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    return 6371000 * c

def safe_request(method: str, url: str, **kwargs):
    """HTTP request with simple rate limiting."""
    time.sleep(RATE_LIMIT_DELAY)
    try:
        response = requests.request(method, url, timeout=30, **kwargs)
        response.raise_for_status()
        return response
    except Exception as e:
        print(f"Request error: {url} - {e}")
        return None

def sanitize_text(text: str) -> str:
    """Normalize whitespace in scraped text."""
    if not text:
        return ""
    return text.strip().replace('\n', ' ').replace('\r', '')

def is_main_departure_time(time_text: str) -> bool:
    """True if cell looks like a main-line time (plain HH:MM), not express markup."""
    if not time_text:
        return False
    
    time_text = time_text.strip()
    
    # Main-line cells are plain HH:MM (optional trailing space).
    # Express variants add suffixes like (-1) or F.
    pattern = r'^\d{2}:\d{2}\s*$'
    
    return bool(re.match(pattern, time_text))

# ==================== FETCH ====================

def get_all_routes(hat_kodu: str) -> List[Dict]:
    """All route variants from GetAllRoute."""
    url = f"https://iett.istanbul/tr/RouteStation/GetAllRoute?rcode={hat_kodu}"
    response = safe_request("GET", url)
    
    if not response:
        return []
    
    try:
        data = response.json()
        print(f"  OK {len(data)} patterns (GetAllRoute)")

        if data:
            print(f"  DEBUG first pattern keys: {list(data[0].keys())}")
            print(f"  DEBUG first pattern: {data[0]}")
        
        return data
    except:
        print("  JSON parse failed")
        return []

def get_scheduled_departure_times(hat_kodu: str) -> Dict[str, List[Dict]]:
    """Main-line departure times per origin (express times filtered out)."""
    url = "https://iett.istanbul/tr/RouteStation/GetScheduledDepartureTimes"
    response = safe_request("POST", url, data={"hCode": hat_kodu})
    
    if not response:
        return {}
    
    soup = BeautifulSoup(response.text, 'html.parser')
    tables = soup.find_all('table', class_='line-table')
    
    departure_by_origin = {}
    
    for table in tables:
        # Header: origin name
        header = table.find('th', class_='routedetailstartend')
        if not header:
            continue
        
        origin = sanitize_text(header.get_text()).replace(' KALKIŞ', '')
        
        if origin not in departure_by_origin:
            departure_by_origin[origin] = []
        
        # Time rows
        rows = table.find('tbody').find_all('tr') if table.find('tbody') else []
        
        for row in rows:
            cells = row.find_all('td')
            if len(cells) >= 3:
                weekday = sanitize_text(cells[0].get_text())
                saturday = sanitize_text(cells[1].get_text())
                sunday = sanitize_text(cells[2].get_text())
                
                # Keep only main-line style cells
                if weekday and is_main_departure_time(weekday):
                    departure_by_origin[origin].append({
                        'time': weekday.strip(),
                        'service_type': 'WEEKDAYS',
                        'origin': origin
                    })
                if saturday and is_main_departure_time(saturday):
                    departure_by_origin[origin].append({
                        'time': saturday.strip(),
                        'service_type': 'SATURDAY',
                        'origin': origin
                    })
                if sunday and is_main_departure_time(sunday):
                    departure_by_origin[origin].append({
                        'time': sunday.strip(),
                        'service_type': 'SUNDAY',
                        'origin': origin
                    })
    
    total_times = sum(len(times) for times in departure_by_origin.values())
    print(f"  OK {total_times} main-line departure times")
    
    return departure_by_origin

def get_stations_for_route(hat_kodu: str) -> Dict[str, List[Dict]]:
    """Main-line stop lists per direction index (GetStationForRoute)."""
    url = f"https://iett.istanbul/tr/RouteStation/GetStationForRoute?hatkod={hat_kodu}"
    response = safe_request("GET", url)
    
    if not response:
        return {}
    
    soup = BeautifulSoup(response.text, 'html.parser')
    directions = soup.find_all('div', class_='col-md-6 col-12')
    
    stations_by_index = {}
    
    for idx, direction in enumerate(directions):
        stations = []
        items = direction.find_all('div', class_='line-pass-item')
        
        for seq, item in enumerate(items, 1):
            link = item.find('a')
            if not link:
                continue
            
            href = link.get('href', '')
            if 'dkod=' in href:
                stop_code = href.split('dkod=')[1].split('&')[0]
                stop_name = sanitize_text(link.find('p').get_text())
                
                stations.append({
                    'sequence': seq,
                    'stop_code': stop_code,
                    'stop_name': stop_name
                })
        
        if stations:
            stations_by_index[idx] = stations
    
    total_stations = sum(len(s) for s in stations_by_index.values())
    print(f"  OK {len(stations_by_index)} main-line directions, {total_stations} stops total")
    
    return stations_by_index

def get_fast_station(hat_kodu: str) -> Dict[str, Dict]:
    """Express (depar) pattern details from GetFastStation."""
    url = f"https://iett.istanbul/tr/RouteStation/GetFastStation?routeid={hat_kodu}"
    response = safe_request("GET", url)
    
    if not response:
        return {}
    
    soup = BeautifulSoup(response.text, 'html.parser')
    options = soup.find_all('div', class_='custom-option-item')
    
    depar_guzergah_data = {}
    
    for option in options:
        guzergah_kodu = option.get('data-content', '')
        if not guzergah_kodu:
            continue
        
        detail_div = soup.find('div', id=guzergah_kodu)
        if not detail_div:
            continue
        
        # Stops
        stations = []
        station_items = detail_div.find_all('div', class_='line-pass-item')
        
        for idx, item in enumerate(station_items, 1):
            link = item.find('a')
            if not link:
                continue
            
            href = link.get('href', '')
            if 'dkod=' in href:
                stop_code = href.split('dkod=')[1].split('&')[0]
                stop_name = sanitize_text(link.find('p').get_text())
                
                stations.append({
                    'sequence': idx,
                    'stop_code': stop_code,
                    'stop_name': stop_name
                })
        
        # Departures
        times = []
        time_table = detail_div.find('table', class_='line-table')
        
        if time_table:
            header = time_table.find('th', class_='routedetaildeparstartend')
            origin = sanitize_text(header.get_text()).replace(' KALKIŞ', '') if header else ''
            
            rows = time_table.find('tbody').find_all('tr') if time_table.find('tbody') else []
            
            for row in rows:
                cells = row.find_all('td')
                if len(cells) >= 3:
                    weekday_cell = cells[0].find('span')
                    saturday_cell = cells[1].find('span')
                    sunday_cell = cells[2].find('span')
                    
                    weekday = sanitize_text(weekday_cell.get_text()) if weekday_cell else ''
                    saturday = sanitize_text(saturday_cell.get_text()) if saturday_cell else ''
                    sunday = sanitize_text(sunday_cell.get_text()) if sunday_cell else ''
                    
                    if weekday:
                        times.append({'time': weekday.strip(), 'service_type': 'WEEKDAYS', 'origin': origin})
                    if saturday:
                        times.append({'time': saturday.strip(), 'service_type': 'SATURDAY', 'origin': origin})
                    if sunday:
                        times.append({'time': sunday.strip(), 'service_type': 'SUNDAY', 'origin': origin})
        
        if stations or times:
            depar_guzergah_data[guzergah_kodu] = {
                'stations': stations,
                'times': times,
                'is_depar': True
            }
    
    print(f"  OK {len(depar_guzergah_data)} express patterns")
    return depar_guzergah_data

# ==================== GTFS BUILD ====================

class GTFSBuilder:
    def __init__(self):
        self.routes = []
        self.trips = []
        self.stop_times = []
        self.stops = []
        self.shapes = []
        self.calendar = []
        self.agency = []
        
        self.trip_counter = 1
        self.existing_stops_df = None
        self.geojson_data = None
        
        self.load_existing_data()
    
    def load_existing_data(self):
        """Load baseline stops CSV and route GeoJSON if present."""
        if os.path.exists(EXISTING_STOPS_CSV):
            self.existing_stops_df = pd.read_csv(EXISTING_STOPS_CSV, dtype={'stop_code': str})
            if 'stop_code' in self.existing_stops_df.columns:
                self.existing_stops_df = self.existing_stops_df[self.existing_stops_df['stop_code'].notna()]
            print(f"OK {len(self.existing_stops_df)} existing stops loaded")
        
        if os.path.exists(GEOJSON_FILE):
            with open(GEOJSON_FILE, 'r', encoding='utf-8') as f:
                self.geojson_data = json.load(f)
            print(f"OK {len(self.geojson_data['features'])} route geometries loaded")
    
    def get_stop_info(self, stop_code: str) -> Dict:
        """Lookup stop row by public stop_code."""
        if self.existing_stops_df is None:
            return None
        
        try:
            stop_code_int = int(stop_code)
            stop = self.existing_stops_df[self.existing_stops_df['stop_code'] == stop_code_int]
            
            if stop.empty:
                stop = self.existing_stops_df[self.existing_stops_df['stop_code'].astype(str) == stop_code]
            
            if stop.empty:
                return None
            
            return stop.iloc[0].to_dict()
        except (ValueError, TypeError):
            return None
    
    def match_shape_id(self, guzergah_kodu: str) -> str:
        """Resolve GeoJSON feature id for a pattern code."""
        if not self.geojson_data:
            return None
        
        # Exact code
        for feature in self.geojson_data['features']:
            if feature['properties']['GUZERGAH_K'] == guzergah_kodu:
                return guzergah_kodu
        
        # Prefix match on base code
        base_code = guzergah_kodu.rsplit('_D', 1)[0]
        
        for feature in self.geojson_data['features']:
            geojson_code = feature['properties']['GUZERGAH_K']
            if geojson_code.startswith(base_code) and feature['properties']['DURUM'] == 'AKTİF':
                return geojson_code
        
        return None
    
    def convert_geojson_to_shapes(self):
        """Flatten GeoJSON LineStrings into shapes.txt rows."""
        if not self.geojson_data:
            return
        
        for feature in self.geojson_data['features']:
            props = feature['properties']
            
            if props['DURUM'] != 'AKTİF':
                continue
            
            shape_id = props['GUZERGAH_K']
            coords = feature['geometry']['coordinates']
            
            sequence = 1
            distance = 0.0
            prev_point = None
            
            for linestring in coords:
                for lon, lat in linestring:
                    self.shapes.append({
                        'shape_id': shape_id,
                        'shape_pt_lat': lat,
                        'shape_pt_lon': lon,
                        'shape_pt_sequence': sequence,
                        'shape_dist_traveled': round(distance, 2)
                    })
                    
                    if prev_point:
                        distance += haversine(prev_point[0], prev_point[1], lat, lon)
                    
                    prev_point = (lat, lon)
                    sequence += 1
        
        print(f"OK {len(set(s['shape_id'] for s in self.shapes))} distinct shapes")
    
    def create_trips_for_guzergah(self, route_id: int, guzergah_kodu: str, stations: List[Dict],
                                   times: List[Dict], is_depar: bool = False):
        """Materialize trips and stop_times for one pattern code."""

        # direction_id from code convention
        direction_id = 0 if '_G_' in guzergah_kodu else 1
        
        # Matched polyline
        shape_id = self.match_shape_id(guzergah_kodu)
        
        # trip_headsign from last stop label
        trip_headsign = stations[-1]['stop_name'].split('.')[1].strip() if '.' in stations[-1]['stop_name'] else stations[-1]['stop_name']
        
        # One trip per scraped departure
        for time_info in times:
            trip_id = self.trip_counter
            self.trip_counter += 1
            
            service_map = {
                'WEEKDAYS': 0,
                'SATURDAY': 6,
                'SUNDAY': 7
            }
            service_id = service_map.get(time_info['service_type'], 0)
            
            # Trip
            self.trips.append({
                'trip_id': trip_id,
                'route_id': route_id,
                'service_id': service_id,
                'trip_headsign': trip_headsign,
                'direction_id': direction_id,
                'shape_id': shape_id if shape_id else '',
                'trip_short_name': guzergah_kodu
            })
            
            # stop_times (time filled only at first stop in this builder)
            departure_time = time_info['time']
            
            for station in stations:
                stop_code = station['stop_code']
                stop_info = self.get_stop_info(stop_code)
                
                if stop_info:
                    # Dedup stops
                    if not any(s['stop_id'] == stop_info['stop_id'] for s in self.stops):
                        self.stops.append({
                            'stop_id': int(stop_info['stop_id']),
                            'stop_code': int(stop_info['stop_code']),
                            'stop_name': stop_info['stop_name'],
                            'stop_desc': stop_info.get('stop_desc', ''),
                            'stop_lat': stop_info['stop_lat'],
                            'stop_lon': stop_info['stop_lon'],
                            'location_type': int(stop_info.get('location_type', 0))
                        })
                    
                    # stop_time row
                    seq = station['sequence']
                    self.stop_times.append({
                        'trip_id': trip_id,
                        'stop_id': int(stop_info['stop_id']),
                        'stop_sequence': seq,
                        'arrival_time': departure_time if seq == 1 else '',
                        'departure_time': departure_time if seq == 1 else '',
                        'timepoint': 1 if seq == 1 else 0
                    })
    
    def process_route(self, hat_kodu: str, route_id: int):
        """Scrape one bus line code: main patterns plus express."""
        print(f"\n{'='*60}")
        print(f"Route: {hat_kodu}")
        print(f"{'='*60}")
        
        # 1) Pattern list
        guzergahlar = get_all_routes(hat_kodu)
        if not guzergahlar:
            print("  WARN no patterns")
            return
        
        # 2) Main-line times
        main_departure_by_origin = get_scheduled_departure_times(hat_kodu)
        
        # 3) Main-line stops
        main_stations_by_index = get_stations_for_route(hat_kodu)
        
        # 4) Express patterns
        depar_guzergahlar = get_fast_station(hat_kodu)
        
        # routes.txt row
        self.routes.append({
            'route_id': route_id,
            'agency_id': 1,
            'route_short_name': hat_kodu,
            'route_type': 3
        })
        
        # 5) Build main-line trips (HTML tables + stop order; not only GetAllRoute)
        
        for idx in sorted(main_stations_by_index.keys()):
            stations = main_stations_by_index[idx]
            
            # First stop label (trim numeric prefix)
            first_stop_name = stations[0]['stop_name']
            if '.' in first_stop_name:
                first_stop_name = first_stop_name.split('.')[1].strip()
            
            print(f"\n  DEBUG main pattern index {idx}")
            print(f"  DEBUG first stop: {first_stop_name}")
            
            # Match departure block to this direction by origin name
            times = []
            matched_origin = None
            
            for origin, origin_times in main_departure_by_origin.items():
                # Substring match on origin / first stop
                if origin in first_stop_name or first_stop_name in origin:
                    times = origin_times
                    matched_origin = origin
                    print(f"  DEBUG matched origin '{origin}' <-> '{first_stop_name}'")
                    break
            
            # Fallback: pair by sorted origin key order
            if not times and idx < len(list(main_departure_by_origin.keys())):
                matched_origin = list(main_departure_by_origin.keys())[idx]
                times = main_departure_by_origin[matched_origin]
                print(f"  DEBUG index fallback origin: {matched_origin}")
            
            if times:
                # Synthetic pattern id like 146T_G_D0 / 146T_D_D0
                last_stop_name = stations[-1]['stop_name']
                if '.' in last_stop_name:
                    last_stop_name = last_stop_name.split('.')[1].strip()
                
                # Heuristic G/D from known terminal names (IETT-specific)
                if 'BOĞAZKÖY' in first_stop_name.upper() or 'BOGAZ' in first_stop_name.upper():
                    direction_letter = 'G'
                    direction_id = 0
                elif 'YENİKAPI' in first_stop_name.upper() or 'YENIKAPI' in first_stop_name.upper():
                    direction_letter = 'D'
                    direction_id = 1
                else:
                    # Unknown: use direction index
                    direction_letter = 'G' if idx == 0 else 'D'
                    direction_id = idx
                
                guzergah_kodu = f"{hat_kodu}_{direction_letter}_D0"
                
                print(f"  DEBUG pattern id: {guzergah_kodu}")
                print(f"  DEBUG {first_stop_name} -> {last_stop_name}")
                
                self.create_trips_for_guzergah(route_id, guzergah_kodu, stations, times, is_depar=False)
                print(f"  OK main pattern {guzergah_kodu} - {len(times)} trips")
            else:
                print(f"  WARN no times for direction index {idx}")

        # 6) Express patterns
        for guzergah_kodu, details in depar_guzergahlar.items():
            stations = details['stations']
            times = details['times']
            
            if stations and times:
                self.create_trips_for_guzergah(route_id, guzergah_kodu, stations, times, is_depar=True)
                print(f"  OK express {guzergah_kodu} - {len(times)} trips")

        total_trips = len([t for t in self.trips if t['route_id'] == route_id])
        print(f"OK {hat_kodu} done - {total_trips} trips for this route")
    
    def create_calendar(self):
        """Fixed weekday / Saturday / Sunday services."""
        self.calendar = [
            {
                'service_id': 0,
                'monday': 1,
                'tuesday': 1,
                'wednesday': 1,
                'thursday': 1,
                'friday': 1,
                'saturday': 0,
                'sunday': 0,
                'start_date': '20241001',
                'end_date': '20251231'
            },
            {
                'service_id': 6,
                'monday': 0,
                'tuesday': 0,
                'wednesday': 0,
                'thursday': 0,
                'friday': 0,
                'saturday': 1,
                'sunday': 0,
                'start_date': '20241001',
                'end_date': '20251231'
            },
            {
                'service_id': 7,
                'monday': 0,
                'tuesday': 0,
                'wednesday': 0,
                'thursday': 0,
                'friday': 0,
                'saturday': 0,
                'sunday': 1,
                'start_date': '20241001',
                'end_date': '20251231'
            }
        ]
    
    def create_agency(self):
        """Single agency row for IETT."""
        self.agency = [{
            'agency_id': 1,
            'agency_name': 'İETT',
            'agency_url': 'https://iett.istanbul',
            'agency_timezone': 'Europe/Istanbul',
            'agency_lang': 'tr'
        }]
    
    def save_gtfs(self):
        """Write tables to OUTPUT_DIR."""
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        files = {
            'agency.txt': self.agency,
            'calendar.txt': self.calendar,
            'routes.txt': self.routes,
            'trips.txt': self.trips,
            'stop_times.txt': self.stop_times,
            'stops.txt': self.stops,
            'shapes.txt': self.shapes if self.shapes else None
        }
        
        for filename, data in files.items():
            if data:
                with open(f'{OUTPUT_DIR}/{filename}', 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=data[0].keys())
                    writer.writeheader()
                    writer.writerows(data)
        
        print(f"\n{'='*60}")
        print("GTFS files written:")
        for filename, data in files.items():
            if data:
                print(f"  - {filename}: {len(data)} rows")
        print(f"{'='*60}")

# ==================== MAIN ====================

def main():
    print("="*60)
    print("IETT GTFS builder")
    print("="*60)
    
    builder = GTFSBuilder()
    builder.create_agency()
    builder.create_calendar()
    builder.convert_geojson_to_shapes()
    
    if TEST_MODE:
        hat_kodlari = ['146T']
        print("\nTEST MODE: only route 146T\n")
    else:
        if os.path.exists(EXISTING_ROUTES_CSV):
            routes_df = pd.read_csv(EXISTING_ROUTES_CSV)
            hat_kodlari = routes_df['route_short_name'].unique().tolist()
            print(f"\nOK {len(hat_kodlari)} routes from CSV\n")
        else:
            print("ERROR routes.csv not found")
            return
    
    for idx, hat_kodu in enumerate(hat_kodlari, 1):
        try:
            builder.process_route(hat_kodu, route_id=idx)
        except Exception as e:
            print(f"ERROR ({hat_kodu}): {e}")
            import traceback
            traceback.print_exc()
    
    builder.save_gtfs()
    
    print("\nFinished.")
    print(f"Output directory: {OUTPUT_DIR}/")

if __name__ == "__main__":
    main()