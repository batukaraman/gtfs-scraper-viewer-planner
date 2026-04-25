"""Download OSM data from Geofabrik."""

from __future__ import annotations

import logging
import shutil
from pathlib import Path

import requests

logger = logging.getLogger(__name__)


class OSMDownloader:
    """Download OSM PBF files from Geofabrik."""

    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def download(
        self,
        url: str,
        filename: str,
        force: bool = False,
    ) -> Path:
        """Download OSM PBF file.
        
        Args:
            url: Download URL (e.g., Geofabrik)
            filename: Output filename
            force: Overwrite existing file
        
        Returns:
            Path to downloaded file
        """
        output_path = self.output_dir / filename

        if output_path.exists() and not force:
            logger.info("OSM file already exists: %s", output_path)
            return output_path

        logger.info("Downloading OSM data from %s...", url)
        logger.info("This may take a while for large regions...")

        try:
            with requests.get(url, stream=True, timeout=3600) as r:
                r.raise_for_status()
                total_size = int(r.headers.get("content-length", 0))

                with open(output_path, "wb") as f:
                    downloaded = 0
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total_size > 0:
                            pct = (downloaded / total_size) * 100
                            if downloaded % (50 * 1024 * 1024) < 8192:
                                logger.info("Downloaded %.1f%% (%.1f MB)", pct, downloaded / 1024 / 1024)

            size_mb = output_path.stat().st_size / 1024 / 1024
            logger.info("Download complete: %s (%.1f MB)", output_path, size_mb)

        except Exception as e:
            if output_path.exists():
                output_path.unlink()
            raise RuntimeError(f"Download failed: {e}") from e

        return output_path

    def is_available(self, filename: str) -> bool:
        """Check if OSM file already exists."""
        return (self.output_dir / filename).exists()

    def get_path(self, filename: str) -> Path:
        """Get path to OSM file."""
        return self.output_dir / filename
