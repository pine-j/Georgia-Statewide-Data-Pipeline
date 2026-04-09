"""Generate current-year AADT coverage audit artifacts from the cleaned CSV."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from normalize import PROJECT_ROOT, write_current_aadt_coverage_audit

LOGGER = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    csv_path = (
        PROJECT_ROOT
        / "02-Data-Staging"
        / "cleaned"
        / "roadway_inventory_cleaned.csv"
    )
    if not csv_path.exists():
        raise FileNotFoundError(f"Cleaned CSV not found: {csv_path}")

    LOGGER.info("Reading cleaned roadway CSV for current AADT coverage audit: %s", csv_path)
    df = pd.read_csv(csv_path, low_memory=False)
    write_current_aadt_coverage_audit(df)


if __name__ == "__main__":
    main()
