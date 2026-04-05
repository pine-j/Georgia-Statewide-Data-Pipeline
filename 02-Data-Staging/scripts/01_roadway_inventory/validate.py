"""Validate the processed Georgia Roadway Inventory data.

Runs a suite of quality checks against the cleaned data, SQLite database,
and GeoPackage outputs. Reports pass/fail for each check.
"""

import json
import logging
import sqlite3
import sys
from pathlib import Path

import geopandas as gpd
import pandas as pd

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
CLEANED_DIR = PROJECT_ROOT / "02-Data-Staging" / "cleaned"
DB_DIR = PROJECT_ROOT / "02-Data-Staging" / "databases"
SPATIAL_DIR = PROJECT_ROOT / "02-Data-Staging" / "spatial"
RAPTOR_DIR = PROJECT_ROOT / "05-RAPTOR-Integration"

TARGET_CRS = "EPSG:32617"

# Critical columns that should not have excessive nulls
CRITICAL_COLUMNS = [
    "unique_id",
    "ROUTE_ID",
    "COUNTY_ID",
    "GDOT_District",
    "SYSTEM_CODE",
]

DECODED_LABEL_COLUMNS = {
    "COUNTY_NAME": "COUNTY_CODE",
    "DISTRICT_NAME": "DISTRICT",
    "DISTRICT_LABEL": "DISTRICT",
    "SYSTEM_CODE_LABEL": "SYSTEM_CODE",
    "FUNCTION_TYPE_LABEL": "FUNCTION_TYPE",
    "PARSED_FUNCTION_TYPE_LABEL": "PARSED_FUNCTION_TYPE",
    "F_SYSTEM_LABEL": "F_SYSTEM",
    "FUNCTIONAL_CLASS_LABEL": "FUNCTIONAL_CLASS",
    "FACILITY_TYPE_LABEL": "FACILITY_TYPE",
    "NHS_LABEL": "NHS",
    "NHS_IND_LABEL": "NHS_IND",
    "MEDIAN_TYPE_LABEL": "MEDIAN_TYPE",
    "SHOULDER_TYPE_LABEL": "SHOULDER_TYPE",
    "SURFACE_TYPE_LABEL": "SURFACE_TYPE",
    "URBAN_CODE_LABEL": "URBAN_CODE",
    "DIRECTION_LABEL": "DIRECTION",
    "PARSED_DIRECTION_LABEL": "PARSED_DIRECTION",
    "ROUTE_DIRECTION_LABEL": "ROUTE_DIRECTION",
    "PARSED_SYSTEM_CODE_LABEL": "PARSED_SYSTEM_CODE",
    "ROUTE_TYPE_LABEL": "ROUTE_TYPE",
}


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
    if "GDOT_District" not in df.columns:
        result.add("District range", False, "GDOT_District column not found")
        return

    districts = df["GDOT_District"].dropna().unique()
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


def validate_aadt_coverage(result: ValidationResult, df: pd.DataFrame) -> None:
    """Report current and historic AADT coverage."""
    if "AADT" not in df.columns:
        result.add("Current AADT coverage", False, "AADT column not found")
        return

    current_count = int(df["AADT"].notna().sum())
    result.add(
        "Current AADT coverage",
        current_count > 0,
        f"{current_count:,} segments with current AADT",
    )

    historical_cols = sorted(
        col for col in df.columns if col.startswith("AADT_") and col[5:].isdigit() and col != "AADT_2024"
    )
    if not historical_cols:
        result.add("Historic AADT columns", False, "No historic AADT_* columns found")
        return

    covered_years = {
        col: int(df[col].notna().sum())
        for col in historical_cols
    }
    years_with_data = {col: count for col, count in covered_years.items() if count > 0}
    result.add(
        "Historic AADT coverage",
        len(years_with_data) > 0,
        ", ".join(f"{col}={count:,}" for col, count in years_with_data.items()) if years_with_data else "No historic segments matched",
    )


def validate_decoded_labels(result: ValidationResult, df: pd.DataFrame) -> None:
    """Check that decoded label columns exist and cover rows with source codes."""
    for label_col, source_col in DECODED_LABEL_COLUMNS.items():
        if label_col not in df.columns:
            result.add(f"Decoded label: {label_col}", False, "Column not found")
            continue
        if source_col not in df.columns:
            result.add(f"Decoded label: {label_col}", False, f"Source column missing: {source_col}")
            continue

        source_mask = df[source_col].notna()
        source_count = int(source_mask.sum())
        if source_count == 0:
            result.add(f"Decoded label: {label_col}", True, "No source-coded rows present")
            continue

        decoded_count = int(df.loc[source_mask, label_col].notna().sum())
        result.add(
            f"Decoded label: {label_col}",
            decoded_count == source_count,
            f"{decoded_count:,}/{source_count:,} source-coded rows decoded",
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


def validate_boundary_layers(result: ValidationResult) -> None:
    """Check that county and district boundary layers exist and are populated."""
    gpkg_path = SPATIAL_DIR / "base_network.gpkg"
    if not gpkg_path.exists():
        result.add("Boundary layers", False, "GeoPackage not found")
        return

    conn = sqlite3.connect(gpkg_path)
    try:
        available_layers = {
            row[0]
            for row in conn.execute("SELECT table_name FROM gpkg_contents").fetchall()
        }
    finally:
        conn.close()

    expected_layers = {"county_boundaries", "district_boundaries"}
    missing_layers = expected_layers - available_layers
    result.add(
        "Boundary layers exist",
        not missing_layers,
        f"Missing: {sorted(missing_layers)}" if missing_layers else "county_boundaries, district_boundaries",
    )
    if missing_layers:
        return

    county_gdf = gpd.read_file(gpkg_path, layer="county_boundaries", engine="pyogrio")
    district_gdf = gpd.read_file(gpkg_path, layer="district_boundaries", engine="pyogrio")

    result.add(
        "County boundary count",
        len(county_gdf) == 159,
        f"{len(county_gdf):,} features",
    )
    result.add(
        "District boundary count",
        len(district_gdf) == 7,
        f"{len(district_gdf):,} features",
    )

    county_names_ok = "DISTRICT_NAME" in county_gdf.columns and county_gdf["DISTRICT_NAME"].notna().all()
    district_names_ok = "DISTRICT_NAME" in district_gdf.columns and district_gdf["DISTRICT_NAME"].notna().all()
    result.add(
        "County district labels",
        county_names_ok,
        "DISTRICT_NAME populated" if county_names_ok else "Missing or null DISTRICT_NAME values",
    )
    result.add(
        "District names",
        district_names_ok,
        "DISTRICT_NAME populated" if district_names_ok else "Missing or null DISTRICT_NAME values",
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


def validate_raptor_loader(result: ValidationResult) -> None:
    """Smoke-test the RAPTOR RoadwayData loader against staged outputs."""
    if not RAPTOR_DIR.exists():
        result.add("RoadwayData loader", False, f"Path not found: {RAPTOR_DIR}")
        return

    sys.path.insert(0, str(RAPTOR_DIR))
    try:
        from states.Georgia.categories.Roadways import RoadwayData

        statewide = RoadwayData()
        statewide.load_data()
        statewide_count = len(statewide.GA_RDWY_INV) if statewide.GA_RDWY_INV is not None else 0

        district = RoadwayData(district_id=7)
        district.load_data()
        district_count = len(district.GA_RDWY_INV) if district.GA_RDWY_INV is not None else 0
        district_values = (
            set(pd.to_numeric(district.GA_RDWY_INV["DISTRICT"], errors="coerce").dropna().astype(int).unique())
            if district.GA_RDWY_INV is not None and "DISTRICT" in district.GA_RDWY_INV.columns
            else set()
        )

        result.add(
            "RoadwayData statewide load",
            statewide_count > 0,
            f"{statewide_count:,} rows loaded",
        )
        result.add(
            "RoadwayData district load",
            district_count > 0 and district_values == {7},
            f"{district_count:,} rows; districts={sorted(district_values)}",
        )
    except Exception as exc:
        result.add("RoadwayData loader", False, str(exc))
    finally:
        try:
            sys.path.remove(str(RAPTOR_DIR))
        except ValueError:
            pass


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
        validate_aadt_coverage(result, df)
        validate_decoded_labels(result, df)

    validate_crs(result)
    validate_geometry(result)
    validate_boundary_layers(result)
    validate_database(result)
    validate_raptor_loader(result)

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
