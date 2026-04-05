"""Quick script to load only stop_times with missing stops handling."""
import sys
sys.path.insert(0, 'src')

from database import GTFSLoader
from pathlib import Path

loader = GTFSLoader(
    Path('gtfs'),
    'postgresql://gtfs_admin:gtfs_secure_2026@localhost:5432/gtfs_transit'
)

print("Loading stop_times.txt only...")
success = loader.load_file('stop_times.txt')

if success:
    print("SUCCESS!")
    stats = loader.get_stats()
    print(stats[stats['table_name'] == 'stop_times'])
else:
    print("FAILED!")
    sys.exit(1)
