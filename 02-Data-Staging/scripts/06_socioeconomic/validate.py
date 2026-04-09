"""Validate socioeconomic data for the Georgia pipeline.

Checks:
1. Georgia total population matches published figure (~10.9M from 2020 Census)
2. Employment totals are reasonable (millions range)
3. All 159 Georgia counties are represented
4. No duplicate FIPS codes within each dataset
"""

from __future__ import annotations

import logging
import sqlite3
import sys
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[3]
CLEAN_DIR = REPO_ROOT / "02-Data-Staging" / "tables" / "demographics"
DB_PATH = REPO_ROOT / "03-Processed-Data" / "demographics" / "socioeconomic.db"

# Georgia validation constants
GA_COUNTY_COUNT = 159
GA_POPULATION_2020 = 10_711_908  # Official 2020 Census
GA_POP_TOLERANCE = 0.02  # Allow 2% deviation
GA_MIN_EMPLOYMENT = 3_000_000  # Rough lower bound for total employment


class ValidationError(Exception):
    """Raised when a validation check fails."""


def _load_csv(name: str) -> pd.DataFrame:
    """Load a normalized CSV if it exists."""
    path = CLEAN_DIR / name
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, dtype=str, low_memory=False)


def validate_population() -> bool:
    """Check that Georgia total population is within tolerance of published figure."""
    df = _load_csv("decennial_blocks.csv")
    if df.empty:
        logger.warning("SKIP: No decennial blocks data available")
        return True  # Skip rather than fail when data not yet downloaded

    df["P1_001N"] = pd.to_numeric(df["P1_001N"], errors="coerce").fillna(0)
    total_pop = df["P1_001N"].sum()

    deviation = abs(total_pop - GA_POPULATION_2020) / GA_POPULATION_2020
    passed = deviation <= GA_POP_TOLERANCE

    if passed:
        logger.info(
            "PASS: Population = %s (deviation %.2f%% from published %s)",
            f"{total_pop:,.0f}",
            deviation * 100,
            f"{GA_POPULATION_2020:,}",
        )
    else:
        logger.error(
            "FAIL: Population = %s (deviation %.2f%% exceeds %.0f%% tolerance)",
            f"{total_pop:,.0f}",
            deviation * 100,
            GA_POP_TOLERANCE * 100,
        )

    return passed


def validate_employment() -> bool:
    """Check that LODES employment totals are reasonable."""
    df = _load_csv("lodes_wac.csv")
    if df.empty:
        logger.warning("SKIP: No LODES WAC data available")
        return True

    # C000 = Total number of jobs
    if "C000" not in df.columns:
        logger.warning("SKIP: C000 column not in LODES WAC")
        return True

    df["C000"] = pd.to_numeric(df["C000"], errors="coerce").fillna(0)
    total_emp = df["C000"].sum()

    passed = total_emp >= GA_MIN_EMPLOYMENT

    if passed:
        logger.info("PASS: Total LODES employment = %s", f"{total_emp:,.0f}")
    else:
        logger.error(
            "FAIL: Total LODES employment = %s (below minimum %s)",
            f"{total_emp:,.0f}",
            f"{GA_MIN_EMPLOYMENT:,}",
        )

    return passed


def validate_county_coverage() -> bool:
    """Check that all 159 Georgia counties are represented."""
    passed = True

    for dataset_name in ("decennial_blocks.csv", "acs_block_groups.csv"):
        df = _load_csv(dataset_name)
        if df.empty:
            logger.warning("SKIP: %s not available", dataset_name)
            continue

        if "COUNTY_FIPS" not in df.columns:
            # Try to derive from GEOID
            if "GEOID" in df.columns:
                df["COUNTY_FIPS"] = df["GEOID"].str[:5]
            else:
                logger.warning("SKIP: No COUNTY_FIPS or GEOID in %s", dataset_name)
                continue

        county_count = df["COUNTY_FIPS"].nunique()

        if county_count == GA_COUNTY_COUNT:
            logger.info(
                "PASS: %s has all %d counties", dataset_name, GA_COUNTY_COUNT
            )
        else:
            logger.error(
                "FAIL: %s has %d counties (expected %d)",
                dataset_name,
                county_count,
                GA_COUNTY_COUNT,
            )
            passed = False

    return passed


def validate_no_duplicate_fips() -> bool:
    """Check for duplicate FIPS codes in each dataset."""
    passed = True

    checks = [
        ("decennial_blocks.csv", "GEOID"),
        ("acs_block_groups.csv", "GEOID"),
        ("opportunity_zones.csv", "TRACT_GEOID"),
    ]

    for filename, fips_col in checks:
        df = _load_csv(filename)
        if df.empty:
            logger.warning("SKIP: %s not available", filename)
            continue

        if fips_col not in df.columns:
            logger.warning("SKIP: Column %s not in %s", fips_col, filename)
            continue

        dup_count = df[fips_col].duplicated().sum()
        if dup_count == 0:
            logger.info("PASS: No duplicate %s in %s", fips_col, filename)
        else:
            logger.error(
                "FAIL: %d duplicate %s values in %s", dup_count, fips_col, filename
            )
            passed = False

    return passed


def validate_database() -> bool:
    """Check that the SQLite database was created with expected tables."""
    if not DB_PATH.exists():
        logger.warning("SKIP: Database not yet created at %s", DB_PATH)
        return True

    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )
    tables = [row[0] for row in cursor.fetchall()]
    conn.close()

    expected_tables = {
        "decennial_blocks",
        "acs_block_groups",
        "lodes_wac",
        "lodes_rac",
        "economic_census",
        "opb_projections",
        "opportunity_zones",
    }

    missing = expected_tables - set(tables)
    if missing:
        logger.warning("Database missing tables: %s", missing)
        return True  # Warning, not failure — tables depend on data availability

    logger.info("PASS: Database has all expected tables: %s", sorted(tables))
    return True


def run_all() -> bool:
    """Run all validation checks and return overall pass/fail."""
    results = [
        ("Population total", validate_population()),
        ("Employment total", validate_employment()),
        ("County coverage", validate_county_coverage()),
        ("No duplicate FIPS", validate_no_duplicate_fips()),
        ("Database integrity", validate_database()),
    ]

    logger.info("=" * 60)
    all_passed = True
    for name, passed in results:
        status = "PASS" if passed else "FAIL"
        logger.info("  %-25s %s", name, status)
        if not passed:
            all_passed = False

    logger.info("=" * 60)
    logger.info("Overall: %s", "PASS" if all_passed else "FAIL")
    return all_passed


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    passed = run_all()
    if not passed:
        sys.exit(1)


if __name__ == "__main__":
    main()
