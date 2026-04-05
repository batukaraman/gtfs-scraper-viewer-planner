"""
Command-line interface for database operations.

Usage:
    python -m database load       # Load GTFS data
    python -m database test       # Test connection
"""

import sys
import argparse
from pathlib import Path
from dotenv import load_dotenv
import os

from .loader import GTFSLoader
from .test import test_connection as run_test


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="GTFS Database Tools",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    
    # Load command
    load_parser = subparsers.add_parser('load', help='Load GTFS data into database')
    load_parser.add_argument(
        '--gtfs-dir',
        type=Path,
        default=Path('gtfs'),
        help='Directory containing GTFS files (default: gtfs/)'
    )
    load_parser.add_argument(
        '--database-url',
        type=str,
        help='PostgreSQL connection string (or set DATABASE_URL env var)'
    )
    
    # Test command
    test_parser = subparsers.add_parser('test', help='Test database connection')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    # Load environment variables
    load_dotenv()
    
    if args.command == 'load':
        database_url = args.database_url or os.getenv('DATABASE_URL')
        
        if not database_url:
            print("[ERROR] DATABASE_URL not set")
            print("Set DATABASE_URL environment variable or use --database-url flag")
            sys.exit(1)
        
        if not args.gtfs_dir.exists():
            print(f"[ERROR] GTFS directory not found: {args.gtfs_dir}")
            sys.exit(1)
        
        print(f"GTFS directory: {args.gtfs_dir}")
        print(f"Database: {database_url.split('@')[1] if '@' in database_url else 'localhost'}")
        print()
        
        loader = GTFSLoader(args.gtfs_dir, database_url)
        results = loader.load_all()
        
        # Show statistics
        print("\nDatabase Statistics:")
        print("="*60)
        stats = loader.get_stats()
        for _, row in stats.iterrows():
            print(f"{row['table_name']:20s} {row['row_count']:>12,} rows")
        
        if all(results.values()):
            print("\n[SUCCESS] All files loaded successfully!")
            sys.exit(0)
        else:
            print("\n[WARNING] Some files failed to load")
            sys.exit(1)
    
    elif args.command == 'test':
        run_test()


if __name__ == '__main__':
    main()
