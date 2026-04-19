"""Pytest config for aadt_historic_model tests.

Adds the scripts/07_aadt_historic_model package onto sys.path so tests can
import the pipeline modules without installing.
"""

from pathlib import Path
import sys

_SCRIPTS = Path(__file__).resolve().parents[2] / "scripts" / "07_aadt_historic_model"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))
