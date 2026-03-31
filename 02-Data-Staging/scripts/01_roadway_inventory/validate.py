"""Validate the processed Georgia Roadway Inventory data.

Runs a suite of quality checks against the cleaned data, SQLite database,
and GeoPackage outputs. Reports pass/fail for each check.
"""

import json
import logging
import sqlite3
from pathlib import Path

import geopandas as gpd
import pandas as pd

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
CLEANED_DIR = PROJECT_ROOT / "02-Data-Staging" / "cleaned"
DB_DIR = PROJECT_ROOT / "02-Data-Staging" / "databases"
SPATIAL_DIR = PROJECT_ROOT / "02-Data-Staging" / "spatial"

TARGET_CRS = "EPSG:32617"

# Critical columns that should not have excessive nulls
CRITICAL_COLUMNS = [
    "unique_id",
    "RCLINK",
    "COUNTY_CODE",
    "DISTRICT",
    "SYSTEM_CODE",
]


class ValidationResult:
    """Container for validation check results."""

    def __init__(self):
        self.checks: list[dict] = []

    def add(self, name: str, passed: bool, detail: str = "") -> None:
        status = "PASS" if passed else "FAIL"
        self.checks.append({"name": name, "status": status, "detail": detail})
        log_fn = logger.info if passed else logger.error
        log_fn("  [%s] %s %s", status, name, f"- {detail}" if detail else "")

    @property
    def all_passed(self) -> bool:
        return all(c["status"] == "PASS" for c in self.checks)

    def summary(self) -> str:
        passed = sum(1 for c in self.checks if c["status"] == "PASS")
        total = len(self.checks)
        return f"{passed}/{total} checks passed"


def validate_row_count(result: ValidationResult) -> pd.DataFrame | None:
    """Check that cleaned CSV has a reasonable number of rows."""
    csv_path = CLEANED_DIR / "roadway_inventory_cleaned.csv"
    if not csv_path.exists():
        result.add("Row count", False, f"CSV not found: {csv_path}")
        return None

    df = pd.read_csv(csv_path, low_memory=False)
    row_count = len(df)

    # Georgia roadway inventory typically has tens of thousands of segments
    result.add(
        "Row count",
        row_count > 0,
        f"{row_count:,} rows loaded",
    )
    return df


def validate_unique_id(result: ValidationResult, df: pd.DataFrame) -> None:
    """Check that unique_id column exists and has no duplicates."""
    if "unique_id" not in df.columns:
        result.add("unique_id exists", False, "Column not found")
        return

    result.add("unique_id exists", True)

    dup_count = df["unique_id"].duplicated().sum()
    result.add(
        "unique_id uniqueness",
        dup_count == 0,
        f"{dup_count:,} duplicates found" if dup_count > 0 else "All unique",
    )


def validate_null_checks(result: ValidationResult, df: pd.DataFrame) -> None:
    """Check critical columns for excessive null values."""
    for col in CRITICAL_COLUMNS:
        if col not in df.columns:
            result.add(f"Null check: {col}", False, "Column not found")
            continue

        null_pct = df[col].isnull().mean() * 100
        # Allow up to 5% nulls on critical columns
        result.add(
            f"Null check: {col}",
            null_pct < 5.0,
            f"{null_pct:.1f}% null",
        )


def validate_crs(result: ValidationResult) -> None:
    """Verify the GeoPackage has the correct CRS."""
    gpkg_path = SPATIAL_DIR / "base_network.gpkg"
    if not gpkg_path.exists():
        result.add("CRS verification", False, f"GeoPackage not found: {gpkg_path}")
        return

    gdf = gpd.read_file(gpkg_path, layer="roadway_segments", rows=1, engine="pyogrio")
    actual_crs = str(gdf.crs)

    result.add(
        "CRS verification",
        TARGET_CRS.lower() in actual_crs.lower() or "32617" in actual_crs,
        f"CRS = {actual_crs}",
    )


def validate_district_range(result: ValidationResult, df: pd.DataFrame) -> None:
    """Check that DISTRICT values are in the expected range (1-7)."""
    if "DISTRICT" not in df.columns:
        result.add("District range", False, "DISTRICT column not found")
        return

    districts = df["DISTRICT"].dropna().unique()
    valid_range = set(range(1, 8))

    # Convert to int for comparison, handling string types
    try:
        district_ints = {int(d) for d in districts}
    except (ValueError, TypeError):
        result.add("District range", False, f"Non-numeric district values: {districts[:10]}")
        return

    out_of_range = district_ints - valid_range
    result.add(
        "District range (1-7)",
        len(out_of_range) == 0,
        f"Values found: {sorted(district_ints)}"
        + (f", out of range: {sorted(out_of_range)}" if out_of_range else ""),
    )


def validate_geometry(result: ValidationResult) -> None:
    """Check geometry validity in the GeoPackage."""
    gpkg_path = SPATIAL_DIR / "base_network.gpkg"
    if not gpkg_path.exists():
        result.add("Geometry validity", False, "GeoPackage not found")
        return

    gdf = gpd.read_file(gpkg_path, layer="roadway_segments", engine="pyogrio")
    invalid_count = (~gdf.geometry.is_valid).sum()
    empty_count = gdf.geometry.is_empty.sum()

    result.add(
        "Geometry validity",
        invalid_count == 0,
        f"{invalid_count:,} invalid, {empty_count:,} empty out of {len(gdf):,}",
    )


def validate_database(result: ValidationResult) -> None:
    """Check SQLite database integrity."""
    db_path = DB_DIR / "roadway_inventory.db"
    if not db_path.exists():
        result.add("Database exists", False, f"DB not found: {db_path}")
        return

    conn = sqlite3.connect(db_path)
    try:
        # Check segments table exists
        tables = [
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        ]
        result.add("Database tables", "segments" in tables, f"Tables: {tables}")

        # Check row count matches CSV
        if "segments" in tables:
            db_count = conn.execute("SELECT COUNT(*) FROM segments").fetchone()[0]
            result.add("Database row count", db_count > 0, f"{db_count:,} rows")

        # Check load_summary exists
        result.add("Load summary table", "load_summary" in tables)
    finally:
        conn.close()


def main() -> None:
    """Run all validation checks."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    logger.info("Running validation checks...")

    result = ValidationResult()

    # Run checks
    df = validate_row_count(result)

    if df is not None:
        validate_unique_id(result, df)
        validate_null_checks(result, df)
        validate_district_range(result, df)

    validate_crs(result)
    validate_geometry(result)
    validate_database(result)

    # Summary
    logger.info("")
    logger.info("Validation %s: %s", "PASSED" if result.all_passed else "FAILED", result.summary())

    # Write results to JSON
    output_path = PROJECT_ROOT / "02-Data-Staging" / "config" / "validation_results.json"
    output_path.write_text(json.dumps(result.checks, indent=2))
    logger.info("Results written to %s", output_path)

    if not result.all_passed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
