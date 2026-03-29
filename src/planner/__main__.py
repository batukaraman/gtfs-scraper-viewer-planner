"""Run: ``python -m planner`` → Streamlit trip planner."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def main() -> None:
    app = Path(__file__).resolve().parent / "app.py"
    src_root = Path(__file__).resolve().parent.parent
    env = os.environ.copy()
    prev = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = str(src_root) if not prev else f"{src_root}{os.pathsep}{prev}"
    raise SystemExit(
        subprocess.call(
            [sys.executable, "-m", "streamlit", "run", str(app), *sys.argv[1:]],
            env=env,
        )
    )


if __name__ == "__main__":
    main()
