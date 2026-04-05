#!/usr/bin/env python3
"""
GTFS Data Ingestion Script
Loads GTFS CSV files into PostgreSQL + PostGIS database with upsert support.
"""

import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterator, List, Set
from io import StringIO
import logging

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Rows per chunk for streaming COPY (override with GTFS_LOAD_CHUNK_ROWS).
_DEFAULT_CHUNK_ROWS = 250_000
CHUNK_ROWS = max(10_000, int(os.environ.get("GTFS_LOAD_CHUNK_ROWS", str(_DEFAULT_CHUNK_ROWS))))
_UPSERT_BATCH = max(500, int(os.environ.get("GTFS_UPSERT_BATCH", "2500")))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def _series_gtfs_times_to_seconds(s: pd.Series) -> pd.Series:
    """Vectorized HH:MM:SS → seconds since midnight (nullable integers)."""
    str_s = s.astype(str)
    empty = str_s.isin(["", "nan", "NaT", "None", "<NA>"])
    parts = str_s.str.split(":", expand=True)
    if parts.shape[1] < 3:
        if (~empty).any():
            logger.warning(
                "Invalid time format in %s rows (expected HH:MM:SS)",
                int((~empty).sum()),
            )
        return pd.Series(pd.NA, index=s.index, dtype="Int64")
    h = pd.to_numeric(parts[0], errors="coerce")
    m = pd.to_numeric(parts[1], errors="coerce")
    sec = pd.to_numeric(parts[2], errors="coerce")
    out = (h * 3600 + m * 60 + sec).astype("Int64")
    out = out.mask(empty, pd.NA)
    invalid = (~empty) & out.isna()
    if invalid.any():
        n = int(invalid.sum())
        sample = str_s[invalid].head(3).tolist()
        logger.warning("Invalid time format in %s rows, e.g. %s", n, sample)
    return out


def _read_csv_engine_kw() -> Dict:
    """Prefer PyArrow CSV parser when available (much faster on large files)."""
    try:
        import pyarrow  # noqa: F401

        return {"engine": "pyarrow"}
    except ImportError:
        return {}


class GTFSLoader:
    """Load GTFS files into PostgreSQL database with upsert support."""
    
    # GTFS file to table mapping
    FILE_TABLE_MAP = {
        'agency.txt': 'agency',
        'stops.txt': 'stops',
        'routes.txt': 'routes',
        'trips.txt': 'trips',
        'stop_times.txt': 'stop_times',
        'calendar.txt': 'calendar',
        'calendar_dates.txt': 'calendar_dates',
        'shapes.txt': 'shapes',
        'transfers.txt': 'transfers',
        'fare_attributes.txt': 'fare_attributes',
        'fare_rules.txt': 'fare_rules',
        'feed_info.txt': 'feed_info',
        'frequencies.txt': 'frequencies',
    }
    
    # Primary keys for each table (for conflict resolution)
    PRIMARY_KEYS = {
        'agency': ['agency_id'],
        'stops': ['stop_id'],
        'routes': ['route_id'],
        'trips': ['trip_id'],
        'stop_times': ['trip_id', 'stop_sequence'],
        'calendar': ['service_id'],
        'calendar_dates': ['service_id', 'date'],
        'shapes': ['shape_id', 'shape_pt_sequence'],
        'transfers': ['from_stop_id', 'to_stop_id'],
        'fare_attributes': ['fare_id'],
        'fare_rules': [],  # No natural primary key
        'feed_info': ['feed_id'],  # Use feed_id as unique key
        'frequencies': [],  # Composite key, handled separately
    }
    
    def __init__(self, gtfs_dir: Path, database_url: str):
        """Initialize loader.
        
        Args:
            gtfs_dir: Directory containing GTFS .txt files
            database_url: PostgreSQL connection string
        """
        self.gtfs_dir = gtfs_dir
        self.engine = create_engine(database_url, echo=False)
        logger.info("Connected to database")

    def _read_csv_base_kwargs(self) -> Dict:
        kw = {"dtype": str, "keep_default_na": False}
        kw.update(_read_csv_engine_kw())
        return kw

    def _iter_csv_chunks(
        self, filepath: Path, chunksize: int, **read_csv_extra: Any
    ) -> Iterator[pd.DataFrame]:
        """Try PyArrow CSV first; fall back to the default C parser on failure."""
        variants = [
            {**self._read_csv_base_kwargs(), **read_csv_extra},
            {"dtype": str, "keep_default_na": False, **read_csv_extra},
        ]
        last_err: Exception | None = None
        for kw in variants:
            try:
                reader = pd.read_csv(filepath, chunksize=chunksize, **kw)
                for chunk in reader:
                    yield chunk
                return
            except Exception as e:
                last_err = e
                eng = kw.get("engine", "c")
                logger.warning(
                    "pd.read_csv failed (engine=%s): %s — retrying with fallback parser if available.",
                    eng,
                    e,
                )
        assert last_err is not None
        raise last_err

    def _collect_unique_stop_ids(self, filepath: Path) -> Set[str]:
        """One column scan — avoids loading full stop_times into RAM."""
        seen: Set[str] = set()
        for chunk in self._iter_csv_chunks(
            filepath, CHUNK_ROWS, usecols=["stop_id"]
        ):
            seen.update(chunk["stop_id"].astype(str).unique())
        return seen

    def _load_table_chunked_copy(self, filepath: Path, table: str) -> int:
        """Stream CSV in chunks; COPY each chunk inside a single DB transaction."""
        copy_sql: str = ""
        first_columns: List[str] = []
        total = 0
        chunk_i = 0
        t0 = time.perf_counter()
        raw_conn = self.engine.raw_connection()
        try:
            raw_conn.autocommit = False
            cursor = raw_conn.cursor()
            try:
                for chunk in self._iter_csv_chunks(filepath, CHUNK_ROWS):
                    df = self._prepare_dataframe(chunk, table)
                    if df.empty:
                        continue
                    buf = StringIO()
                    df.to_csv(
                        buf,
                        sep="\t",
                        header=False,
                        index=False,
                        na_rep="\\N",
                    )
                    buf.seek(0)
                    if not copy_sql:
                        first_columns = list(df.columns)
                        cols = ", ".join(df.columns)
                        copy_sql = (
                            f"COPY {table} ({cols}) FROM stdin "
                            "WITH (FORMAT csv, DELIMITER E'\\t', NULL '\\N')"
                        )
                    elif list(df.columns) != first_columns:
                        raise ValueError(
                            f"Column mismatch between CSV chunks in {table}: "
                            f"expected {first_columns}, got {list(df.columns)}"
                        )
                    cursor.copy_expert(copy_sql, buf)
                    total += len(df)
                    chunk_i += 1
                    if chunk_i % 5 == 0:
                        elapsed = time.perf_counter() - t0
                        rps = total / elapsed if elapsed > 0 else 0
                        logger.info(
                            "COPY progress: %s rows (%s chunks, ~%s rows/s)",
                            f"{total:,}",
                            chunk_i,
                            f"{rps:,.0f}",
                        )
                raw_conn.commit()
            except Exception:
                raw_conn.rollback()
                raise
            finally:
                cursor.close()
        finally:
            raw_conn.close()
        elapsed = time.perf_counter() - t0
        logger.info(
            "Chunked COPY finished: %s rows in %s chunks, %.1f s total.",
            f"{total:,}",
            chunk_i,
            elapsed,
        )
        return total

    def _upsert_dataframe_batched(self, upsert_sql: str, df: pd.DataFrame) -> None:
        """INSERT … ON CONFLICT in batches (executemany), not iterrows."""
        cols = df.columns.tolist()
        prep = df[cols].replace({pd.NA: None, np.nan: None})
        prep = prep.astype(object).where(pd.notna(prep), None)
        records = prep.to_dict("records")
        n = len(records)
        stmt = text(upsert_sql)
        with self.engine.begin() as conn:
            batch_num = 0
            for i in range(0, n, _UPSERT_BATCH):
                batch = records[i : i + _UPSERT_BATCH]
                conn.execute(stmt, batch)
                batch_num += 1
                done = min(i + _UPSERT_BATCH, n)
                if done == n or batch_num % 5 == 0:
                    logger.info("Upsert progress: %s / %s rows", done, n)
    
    def _convert_time_columns(self, df: pd.DataFrame, table: str) -> pd.DataFrame:
        """Convert GTFS time strings (HH:MM:SS) to seconds since midnight.
        
        Args:
            df: DataFrame with potential time columns
            table: Table name
            
        Returns:
            DataFrame with time columns converted
        """
        time_columns = []
        
        if table == 'stop_times':
            time_columns = ['arrival_time', 'departure_time']
        elif table == 'frequencies':
            time_columns = ['start_time', 'end_time']
        
        for col in time_columns:
            if col in df.columns:
                df[f"{col}_str"] = df[col].astype(str)
                df[col] = _series_gtfs_times_to_seconds(df[col])

        return df
    
    def _prepare_dataframe(self, df: pd.DataFrame, table: str) -> pd.DataFrame:
        """Prepare DataFrame for database insertion.
        
        Args:
            df: Raw DataFrame from CSV
            table: Target table name
            
        Returns:
            Cleaned DataFrame
        """
        # Convert time columns if needed
        df = self._convert_time_columns(df, table)
        
        # Replace empty strings with None
        df = df.replace('', None)
        
        # Convert date columns to proper format
        date_columns = []
        if table == 'calendar':
            date_columns = ['start_date', 'end_date']
        elif table == 'calendar_dates':
            date_columns = ['date']
        elif table == 'feed_info':
            date_columns = ['feed_start_date', 'feed_end_date']
        
        for col in date_columns:
            if col in df.columns:
                # Convert YYYYMMDD to YYYY-MM-DD
                df[col] = pd.to_datetime(df[col], format='%Y%m%d', errors='coerce')
        
        # Handle numeric columns
        if table == 'stops':
            for col in ['stop_lat', 'stop_lon']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
        
        if table == 'shapes':
            for col in ['shape_pt_lat', 'shape_pt_lon', 'shape_dist_traveled']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
        
        if table == 'fare_attributes':
            if 'price' in df.columns:
                df['price'] = pd.to_numeric(df['price'], errors='coerce')
        
        return df
    
    def _build_upsert_query(self, table: str, columns: list) -> str:
        """Build INSERT ... ON CONFLICT DO UPDATE query.
        
        Args:
            table: Table name
            columns: Column names
            
        Returns:
            SQL query string
        """
        primary_keys = self.PRIMARY_KEYS.get(table, [])
        
        if not primary_keys:
            # No upsert, just insert
            return None
        
        # Build column list
        col_list = ', '.join(columns)
        placeholders = ', '.join([f':{col}' for col in columns])
        
        # Build conflict target
        conflict_target = ', '.join(primary_keys)
        
        # Build update set clause (update all non-PK columns)
        update_cols = [col for col in columns if col not in primary_keys]
        update_set = ', '.join([f'{col} = EXCLUDED.{col}' for col in update_cols])
        
        if not update_set:
            # All columns are PKs, just ignore conflicts
            return f"""
                INSERT INTO {table} ({col_list})
                VALUES ({placeholders})
                ON CONFLICT ({conflict_target}) DO NOTHING
            """
        
        return f"""
            INSERT INTO {table} ({col_list})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_target})
            DO UPDATE SET {update_set}
        """
    
    def _create_missing_stops(self, stop_ids: List[str]) -> None:
        """Create dummy stops for missing stop_ids to satisfy foreign keys.
        
        Args:
            stop_ids: List of missing stop_ids
        """
        if not stop_ids:
            return

        logger.warning(
            "Creating %s dummy stops for missing references (batched)...",
            len(stop_ids),
        )
        insert_sql = text("""
            INSERT INTO stops (stop_id, stop_name, stop_lat, stop_lon)
            VALUES (:stop_id, 'Missing Stop (Auto-created)', 0, 0)
            ON CONFLICT (stop_id) DO NOTHING
        """)
        batch_size = 2000
        with self.engine.begin() as conn:
            for i in range(0, len(stop_ids), batch_size):
                batch = stop_ids[i : i + batch_size]
                conn.execute(insert_sql, [{"stop_id": sid} for sid in batch])
    
    def load_file(self, filename: str) -> bool:
        """Load a single GTFS file into database.
        
        Args:
            filename: GTFS filename (e.g., 'stops.txt')
            
        Returns:
            True if successful, False otherwise
        """
        filepath = self.gtfs_dir / filename
        
        if not filepath.exists():
            logger.warning(f"File not found: {filename}")
            return False
        
        table = self.FILE_TABLE_MAP[filename]
        logger.info(f"Loading {filename} -> {table}...")

        try:
            file_size_mb = filepath.stat().st_size / (1024 * 1024)
            with self.engine.connect() as conn:
                row_count = conn.execute(
                    text(f"SELECT COUNT(*) FROM {table}")
                ).scalar()

            use_streaming_copy = table in ("shapes", "stop_times") and row_count == 0

            if use_streaming_copy:
                logger.info(
                    "%s is empty — streaming chunked COPY (%s rows/chunk, single transaction). "
                    "File: %.1f MiB.",
                    table,
                    f"{CHUNK_ROWS:,}",
                    file_size_mb,
                )
                if table == "stop_times":
                    t_scan = time.perf_counter()
                    logger.info("Pass 1/2: distinct stop_id scan (single column)...")
                    unique_stops_in_file = self._collect_unique_stop_ids(filepath)
                    logger.info(
                        "Distinct stop_id count: %s (scan %.1f s).",
                        f"{len(unique_stops_in_file):,}",
                        time.perf_counter() - t_scan,
                    )
                    existing_stops = pd.read_sql(
                        "SELECT stop_id FROM stops", self.engine
                    )
                    existing_stop_ids = set(existing_stops["stop_id"].astype(str))
                    missing_stops = unique_stops_in_file - existing_stop_ids
                    if missing_stops:
                        logger.warning(
                            "Found %s missing stops, creating dummies...",
                            len(missing_stops),
                        )
                        self._create_missing_stops(list(missing_stops))
                    logger.info("Pass 2/2: full row load + COPY…")

                total_loaded = self._load_table_chunked_copy(filepath, table)
                if total_loaded == 0:
                    logger.warning("Empty file: %s", filename)
                else:
                    logger.info(
                        "✓ Loaded %s rows into %s (chunked COPY, one commit).",
                        f"{total_loaded:,}",
                        table,
                    )
                return True

            logger.info(
                "Reading %s (%.1f MiB on disk). Full in-memory parse (table has %s rows or not a bulk-COPY table).",
                filename,
                file_size_mb,
                f"{row_count:,}",
            )
            t_read = time.perf_counter()
            try:
                df = pd.read_csv(filepath, **self._read_csv_base_kwargs())
            except Exception as e:
                logger.warning(
                    "pd.read_csv failed with fast engine (%s); retrying with default parser.",
                    e,
                )
                df = pd.read_csv(
                    filepath, dtype=str, keep_default_na=False
                )
            read_s = time.perf_counter() - t_read
            n = len(df)
            rps = n / read_s if read_s > 0 else 0
            logger.info(
                "CSV parsed: %s rows in %.1f s (~%s rows/s). Preparing columns…",
                f"{n:,}",
                read_s,
                f"{rps:,.0f}",
            )

            if df.empty:
                logger.warning(f"Empty file: {filename}")
                return True

            t_prep = time.perf_counter()
            df = self._prepare_dataframe(df, table)
            logger.info("Prepared columns in %.1f s.", time.perf_counter() - t_prep)

            if table == "stop_times":
                unique_stops_in_file = set(df["stop_id"].astype(str).unique())
                existing_stops = pd.read_sql(
                    "SELECT stop_id FROM stops", self.engine
                )
                existing_stop_ids = set(existing_stops["stop_id"].astype(str))
                missing_stops = unique_stops_in_file - existing_stop_ids
                if missing_stops:
                    logger.warning(
                        "Found %s missing stops, creating dummies...",
                        len(missing_stops),
                    )
                    self._create_missing_stops(list(missing_stops))

            if table in ("shapes", "stop_times") and row_count > 0:
                logger.info(
                    "Table %s not empty (%s rows); using batched upsert (batch=%s).",
                    table,
                    f"{row_count:,}",
                    _UPSERT_BATCH,
                )

            upsert_query = self._build_upsert_query(table, df.columns.tolist())

            if upsert_query:
                t_up = time.perf_counter()
                self._upsert_dataframe_batched(upsert_query, df)
                logger.info(
                    "✓ Loaded %s rows into %s (batched upsert, %.1f s).",
                    f"{len(df):,}",
                    table,
                    time.perf_counter() - t_up,
                )
            else:
                # Use pandas to_sql (faster for bulk insert)
                # But it doesn't support ON CONFLICT, so may fail on duplicates
                try:
                    df.to_sql(
                        table,
                        self.engine,
                        if_exists='append',
                        index=False,
                        method='multi',
                        chunksize=1000
                    )
                    logger.info(f"✓ Loaded {len(df)} rows into {table}")
                except Exception as e:
                    # If bulk insert fails, try row-by-row with error handling
                    logger.warning(f"Bulk insert failed, trying row-by-row...")
                    successful = 0
                    failed = 0
                    with self.engine.begin() as conn:
                        for _, row in df.iterrows():
                            try:
                                row_dict = row.to_dict()
                                cols = ', '.join(row_dict.keys())
                                vals = ', '.join([f":{k}" for k in row_dict.keys()])
                                insert_sql = f"INSERT INTO {table} ({cols}) VALUES ({vals})"
                                conn.execute(text(insert_sql), row_dict)
                                successful += 1
                            except Exception:
                                failed += 1
                    
                    if failed > 0:
                        logger.warning(f"✓ Loaded {successful}/{len(df)} rows into {table} ({failed} duplicates skipped)")
                    else:
                        logger.info(f"✓ Loaded {successful} rows into {table}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to load {filename}: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def load_all(self) -> Dict[str, bool]:
        """Load all GTFS files in recommended order.
        
        Returns:
            Dictionary mapping filename to success status
        """
        # Load order matters due to foreign key constraints
        load_order = [
            'agency.txt',
            'feed_info.txt',
            'calendar.txt',
            'stops.txt',
            'routes.txt',
            'shapes.txt',
            'trips.txt',
            'stop_times.txt',
            'calendar_dates.txt',
            'transfers.txt',
            'fare_attributes.txt',
            'fare_rules.txt',
            'frequencies.txt',
        ]
        
        results = {}
        
        for filename in load_order:
            results[filename] = self.load_file(filename)
        
        # Print summary
        logger.info("\n" + "="*60)
        logger.info("LOAD SUMMARY")
        logger.info("="*60)
        
        success_count = sum(1 for v in results.values() if v)
        total_count = len(results)
        
        for filename, success in results.items():
            status = "✓" if success else "✗"
            logger.info(f"{status} {filename}")
        
        logger.info("="*60)
        logger.info(f"Success: {success_count}/{total_count}")
        
        # Analyze tables for query planner
        if success_count > 0:
            logger.info("\nAnalyzing tables...")
            with self.engine.begin() as conn:
                conn.execute(text("ANALYZE"))
            logger.info("✓ Database statistics updated")
        
        return results
    
    def get_stats(self) -> pd.DataFrame:
        """Get database statistics.
        
        Returns:
            DataFrame with table row counts
        """
        query = "SELECT * FROM get_gtfs_stats()"
        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn)
        return df


def main():
    """Main entry point."""
    # Load environment variables
    load_dotenv()
    
    # Get configuration
    gtfs_dir = Path(os.getenv('GTFS_DIR', 'gtfs'))
    database_url = os.getenv('DATABASE_URL')
    
    if not database_url:
        logger.error("DATABASE_URL not set in environment")
        logger.error("Please set DATABASE_URL in .env file")
        sys.exit(1)
    
    if not gtfs_dir.exists():
        logger.error(f"GTFS directory not found: {gtfs_dir}")
        sys.exit(1)
    
    logger.info(f"GTFS directory: {gtfs_dir}")
    logger.info(f"Database: {database_url.split('@')[1] if '@' in database_url else 'localhost'}")
    
    # Load data
    loader = GTFSLoader(gtfs_dir, database_url)
    results = loader.load_all()
    
    # Show statistics
    logger.info("\nDatabase Statistics:")
    logger.info("="*60)
    stats = loader.get_stats()
    for _, row in stats.iterrows():
        logger.info(f"{row['table_name']:20s} {row['row_count']:>12,} rows")
    
    # Exit with appropriate code
    if all(results.values()):
        logger.info("\n✓ All files loaded successfully!")
        sys.exit(0)
    else:
        logger.warning("\n⚠ Some files failed to load")
        sys.exit(1)


if __name__ == '__main__':
    main()
