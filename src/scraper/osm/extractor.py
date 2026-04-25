"""Extract city-level OSM data using bounding box."""

from __future__ import annotations

import logging
import shutil
import subprocess
from pathlib import Path

from ..config import BoundingBox

logger = logging.getLogger(__name__)


class OSMExtractor:
    """Extract city-level OSM PBF using osmium via Docker."""

    DOCKER_IMAGE = "debian:bookworm-slim"

    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def extract(
        self,
        input_file: str,
        output_file: str,
        bbox: BoundingBox,
        output_dir: Path | None = None,
        force: bool = False,
    ) -> Path:
        """Extract OSM data for a bounding box using Docker + osmium.
        
        Args:
            input_file: Input PBF filename (relative to data_dir)
            output_file: Output PBF filename
            bbox: Bounding box to extract
            output_dir: Directory for output file (defaults to data_dir)
            force: Overwrite existing file
        
        Returns:
            Path to extracted file
        """
        input_path = self.data_dir / input_file
        out_dir = output_dir if output_dir else self.data_dir
        output_path = out_dir / output_file

        if not input_path.exists():
            raise FileNotFoundError(f"Input OSM file not found: {input_path}")

        if output_path.exists() and not force:
            logger.info("OSM extract already exists: %s", output_path)
            return output_path

        if not self._check_docker():
            raise RuntimeError("Docker is required for OSM extraction")

        bbox_str = bbox.to_osmium_string()
        logger.info("Extracting %s -> %s", input_file, output_path)
        logger.info("Bounding box: %s", bbox_str)

        try:
            input_abs = str(self.data_dir.resolve())
            output_abs = str(out_dir.resolve())
            
            cmd = [
                "docker", "run", "--rm",
                "-v", f"{input_abs}:/input:ro",
                "-v", f"{output_abs}:/output",
                self.DOCKER_IMAGE,
                "bash", "-c",
                f"apt-get update -qq && apt-get install -y -qq osmium-tool && "
                f"osmium extract -b {bbox_str} --strategy complete_ways --overwrite "
                f"/input/{input_file} -o /output/{output_file}"
            ]

            logger.info("Running osmium extract (this may take a few minutes)...")
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=3600,
            )

            if result.returncode != 0:
                logger.error("osmium stderr: %s", result.stderr)
                raise RuntimeError(f"osmium extract failed: {result.stderr}")

            if not output_path.exists():
                raise RuntimeError(f"Output file not created: {output_path}")

            size_mb = output_path.stat().st_size / 1024 / 1024
            logger.info("Extract complete: %s (%.1f MB)", output_path, size_mb)

        except subprocess.TimeoutExpired:
            raise RuntimeError("OSM extraction timed out")
        except Exception as e:
            if output_path.exists():
                output_path.unlink()
            raise

        return output_path

    def _check_docker(self) -> bool:
        """Check if Docker is available."""
        try:
            result = subprocess.run(
                ["docker", "version"],
                capture_output=True,
                timeout=10,
            )
            return result.returncode == 0
        except Exception:
            return False

    def extract_native(
        self,
        input_file: str,
        output_file: str,
        bbox: BoundingBox,
        force: bool = False,
    ) -> Path:
        """Extract using native osmium (if installed).
        
        Args:
            input_file: Input PBF filename
            output_file: Output PBF filename
            bbox: Bounding box to extract
            force: Overwrite existing file
        
        Returns:
            Path to extracted file
        """
        input_path = self.data_dir / input_file
        output_path = self.data_dir / output_file

        if not input_path.exists():
            raise FileNotFoundError(f"Input OSM file not found: {input_path}")

        if output_path.exists() and not force:
            logger.info("OSM extract already exists: %s", output_path)
            return output_path

        if not shutil.which("osmium"):
            raise RuntimeError("osmium not found. Use extract() with Docker instead.")

        bbox_str = bbox.to_osmium_string()

        cmd = [
            "osmium", "extract",
            "-b", bbox_str,
            "--strategy", "complete_ways",
            "--overwrite",
            str(input_path),
            "-o", str(output_path),
        ]

        logger.info("Running: %s", " ".join(cmd))
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)

        if result.returncode != 0:
            raise RuntimeError(f"osmium extract failed: {result.stderr}")

        return output_path
