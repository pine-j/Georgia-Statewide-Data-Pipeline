"""Ensure pipeline package and roadway_inventory scripts are importable."""

import sys
from pathlib import Path

# Add 02-Data-Staging/scripts so `import pipeline` works
_scripts_dir = Path(__file__).resolve().parents[2]  # scripts/
if str(_scripts_dir) not in sys.path:
    sys.path.insert(0, str(_scripts_dir))

# Add 01_roadway_inventory so `import normalize` works (needed by run.py)
_ri_dir = _scripts_dir / "01_roadway_inventory"
if _ri_dir.exists() and str(_ri_dir) not in sys.path:
    sys.path.insert(0, str(_ri_dir))
