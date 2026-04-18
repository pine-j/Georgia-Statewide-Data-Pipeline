"""Validate the processed Georgia Roadway Inventory data.

Runs a suite of quality checks against the normalized data, SQLite database,
and GeoPackage outputs. Reports pass/fail for each check.
"""

import json
import logging
import sqlite3
import sys
from pathlib import Path

import geopandas as gpd
import pandas as pd

from route_family import SIGNED_ROUTE_FAMILIES
from utils import decode_lookup_value

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
TABLES_DIR = PROJECT_ROOT / "02-Data-Staging" / "tables"
DB_DIR = PROJECT_ROOT / "02-Data-Staging" / "databases"
SPATIAL_DIR = PROJECT_ROOT / "02-Data-Staging" / "spatial"
CONFIG_DIR = PROJECT_ROOT / "02-Data-Staging" / "config"
RAPTOR_DIR = PROJECT_ROOT / "05-RAPTOR-Integration"

TARGET_CRS = "EPSG:32617"  # NOTE: crs_config.json exists, but this script does not currently read it.
MIN_ROW_COUNT = 200000
MIN_CURRENT_AADT_COVERAGE = 0.95
MIN_COLUMN_COUNT = 100

COUNTY_CODE_LOOKUP = json.loads((CONFIG_DIR / "county_codes.json").read_text(encoding="utf-8"))
DISTRICT_SHORT_NAME_LOOKUP = {
    int(code): name
    for code, name in json.loads((CONFIG_DIR / "district_codes.json").read_text(encoding="utf-8")).items()
}
ROADWAY_DOMAIN_LABELS = json.loads(
    (CONFIG_DIR / "roadway_domain_labels.json").read_text(encoding="utf-8")
)

# Critical columns that should not have excessive nulls
CRITICAL_COLUMNS = [
    "unique_id",
    "ROUTE_ID",
    "COUNTY_ID",
    "DISTRICT",
    "SYSTEM_CODE",
]

DECODED_LABEL_COLUMNS = {
    "COUNTY_NAME": "COUNTY_CODE",
    "DISTRICT_NAME": "DISTRICT",
    "SYSTEM_CODE_LABEL": "SYSTEM_CODE",
    "FUNCTION_TYPE_LABEL": "FUNCTION_TYPE",
    "FUNCTIONAL_CLASS_LABEL": "FUNCTIONAL_CLASS",
    "FACILITY_TYPE_LABEL": "FACILITY_TYPE",
    "NHS_IND_LABEL": "NHS_IND",
    "OWNERSHIP_LABEL": "OWNERSHIP",
    "STRAHNET_LABEL": "STRAHNET",
    "MEDIAN_TYPE_LABEL": "MEDIAN_TYPE",
    "SHOULDER_TYPE_LABEL": "SHOULDER_TYPE",
    "SURFACE_TYPE_LABEL": "SURFACE_TYPE",
    "URBAN_CODE_LABEL": "URBAN_CODE",
    "DIRECTION_LABEL": "DIRECTION",
    "ROUTE_TYPE_GDOT_LABEL": "ROUTE_TYPE_GDOT",
}

DECODED_LABEL_LOOKUPS = {
    "COUNTY_NAME": (COUNTY_CODE_LOOKUP, 3),
    "DISTRICT_NAME": (DISTRICT_SHORT_NAME_LOOKUP, None),
    "SYSTEM_CODE_LABEL": (ROADWAY_DOMAIN_LABELS["system_code"], None),
    "FUNCTION_TYPE_LABEL": (ROADWAY_DOMAIN_LABELS["function_type"], None),
    "FUNCTIONAL_CLASS_LABEL": (ROADWAY_DOMAIN_LABELS["functional_class"], None),
    "FACILITY_TYPE_LABEL": (ROADWAY_DOMAIN_LABELS["facility_type"], None),
    "NHS_IND_LABEL": (ROADWAY_DOMAIN_LABELS["nhs"], None),
    "OWNERSHIP_LABEL": (ROADWAY_DOMAIN_LABELS["ownership"], None),
    "STRAHNET_LABEL": (ROADWAY_DOMAIN_LABELS["strahnet"], None),
    "MEDIAN_TYPE_LABEL": (ROADWAY_DOMAIN_LABELS["median_type"], None),
    "SHOULDER_TYPE_LABEL": (ROADWAY_DOMAIN_LABELS["shoulder_type"], None),
    "SURFACE_TYPE_LABEL": (ROADWAY_DOMAIN_LABELS["surface_type"], None),
    "URBAN_CODE_LABEL": (ROADWAY_DOMAIN_LABELS["urban_code"], 5),
    "DIRECTION_LABEL": (ROADWAY_DOMAIN_LABELS["route_direction"], None),
    "ROUTE_TYPE_GDOT_LABEL": (ROADWAY_DOMAIN_LABELS["route_type_gdot"], None),
}

EXPECTED_PHASE1_ATTRIBUTE_COLUMNS = [
    "LANE_WIDTH",
    "MEDIAN_WIDTH",
    "OWNERSHIP",
    "SHOULDER_WIDTH_L",
    "SHOULDER_WIDTH_R",
    "STRAHNET",
]

EXPECTED_ROUTE_FAMILY_COLUMNS = [
    "BASE_ROUTE_NUMBER",
    "ROUTE_SUFFIX_LABEL",
    "ROUTE_FAMILY",
    "ROUTE_FAMILY_DETAIL",
    "ROUTE_FAMILY_CONFIDENCE",
    "ROUTE_FAMILY_SOURCE",
]

EXPECTED_GDOT_ROUTE_TYPE_COLUMNS = [
    "ROUTE_TYPE_GDOT",
    "ROUTE_TYPE_GDOT_LABEL",
    "HWY_NAME",
]

EXPECTED_SIGNED_ROUTE_VERIFICATION_COLUMNS = [
    "SIGNED_INTERSTATE_FLAG",
    "SIGNED_US_ROUTE_FLAG",
    "SIGNED_STATE_ROUTE_FLAG",
    "SIGNED_ROUTE_FAMILY_PRIMARY",
    "SECONDARY_SIGNED_ROUTE_FAMILY",
    "TERTIARY_SIGNED_ROUTE_FAMILY",
    "SIGNED_ROUTE_FAMILY_ALL",
    "SIGNED_ROUTE_VERIFY_SOURCE",
    "SIGNED_ROUTE_VERIFY_METHOD",
    "SIGNED_ROUTE_VERIFY_CONFIDENCE",
    "SIGNED_ROUTE_VERIFY_SCORE",
    "SIGNED_ROUTE_VERIFY_NOTES",
]
EXPECTED_CURRENT_AADT_PROVENANCE_COLUMNS = [
    "AADT",
    "AADT_2024_OFFICIAL",
    "AADT_2024_SOURCE",
    "AADT_2024_CONFIDENCE",
    "AADT_2024_FILL_METHOD",
    "current_aadt_official_covered",
    "current_aadt_covered",
]

CURRENT_AADT_AUDIT_SUMMARY_ARTIFACT = (
    PROJECT_ROOT / "02-Data-Staging" / "reports" / "current_aadt_coverage_audit_summary.json"
)
CURRENT_AADT_AUDIT_TMP_DIR = (
    PROJECT_ROOT / ".tmp" / "roadway_inventory" / "current_aadt_audit"
)
CURRENT_AADT_AUDIT_TRANSIENT_ARTIFACTS = [
    CURRENT_AADT_AUDIT_TMP_DIR / "current_aadt_uncovered_segments.csv",
    CURRENT_AADT_AUDIT_TMP_DIR / "current_aadt_uncovered_route_summary.csv",
    CURRENT_AADT_AUDIT_TMP_DIR / "current_aadt_state_system_gap_fill_candidates.csv",
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


def clean_optional_text(value) -> str | None:
    if pd.isna(value):
        return None

    text = str(value).strip()
    if text in {"", "nan", "None"}:
        return None
    return text


def split_county_all(value) -> list[str]:
    text = clean_optional_text(value)
    if text is None:
        return []
    return [part.strip() for part in text.split(",") if part.strip()]


def county_all_has_blank_token(value) -> bool:
    text = clean_optional_text(value)
    if text is None:
        return False
    return any(not part.strip() for part in text.split(","))


def validate_row_count(result: ValidationResult) -> pd.DataFrame | None:
    """Check that normalized CSV has a reasonable number of rows."""
    csv_path = TABLES_DIR / "roadway_inventory_cleaned.csv"
    if not csv_path.exists():
        result.add("Row count", False, f"CSV not found: {csv_path}")
        return None

    df = pd.read_csv(csv_path, low_memory=False)
    row_count = len(df)
    result.add(
        "Row count",
        row_count >= MIN_ROW_COUNT,
        f"{row_count:,} rows loaded; threshold >= {MIN_ROW_COUNT:,}",
    )
    return df


def validate_column_count(result: ValidationResult, df: pd.DataFrame) -> None:
    """Check that the staged CSV exposes the expected column breadth."""

    column_count = len(df.columns)
    result.add(
        "Column count",
        column_count >= MIN_COLUMN_COUNT,
        f"{column_count:,} columns; threshold >= {MIN_COLUMN_COUNT}",
    )


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


def validate_state_system_location_fields(result: ValidationResult, df: pd.DataFrame) -> None:
    """Check that state-system segments have county and district assignments."""
    required_columns = ["SYSTEM_CODE", "COUNTY_ID", "COUNTY_CODE", "DISTRICT"]
    missing_columns = [column for column in required_columns if column not in df.columns]
    if missing_columns:
        result.add(
            "State-system county/district coverage",
            False,
            f"Missing columns: {', '.join(missing_columns)}",
        )
        return

    state_subset = df[df["SYSTEM_CODE"].astype(str) == "1"].copy()
    if state_subset.empty:
        result.add("State-system county/district coverage", True, "No SYSTEM_CODE = 1 rows present")
        return

    missing_mask = (
        state_subset["COUNTY_ID"].isna()
        | state_subset["COUNTY_CODE"].isna()
        | state_subset["DISTRICT"].isna()
    )
    missing_count = int(missing_mask.sum())
    result.add(
        "State-system county/district coverage",
        missing_count == 0,
        f"{len(state_subset) - missing_count:,}/{len(state_subset):,} rows have county and district",
    )


def validate_aadt_coverage(result: ValidationResult, df: pd.DataFrame) -> None:
    """Report current and future AADT coverage."""
    if "AADT" not in df.columns:
        result.add("Current AADT coverage", False, "AADT column not found")
        return

    canonical_current_col = "AADT"
    current_count = int(df[canonical_current_col].notna().sum())
    official_count = (
        int(df["AADT_2024_OFFICIAL"].notna().sum())
        if "AADT_2024_OFFICIAL" in df.columns
        else current_count
    )
    coverage_ratio = (current_count / len(df)) if len(df) else 0.0
    result.add(
        "Current AADT coverage",
        coverage_ratio >= MIN_CURRENT_AADT_COVERAGE,
        (
            f"{current_count:,}/{len(df):,} segments with canonical current AADT "
            f"({coverage_ratio:.2%}); threshold >= {MIN_CURRENT_AADT_COVERAGE:.0%}; "
            f"official={official_count:,}"
        ),
    )

    if "FUTURE_AADT_2044" in df.columns:
        future_count = int(df["FUTURE_AADT_2044"].notna().sum())
        result.add(
            "Future AADT 2044 coverage",
            future_count > 0,
            f"{future_count:,} segments with future AADT (2044 projection)",
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


def validate_decoded_label_correctness(result: ValidationResult, df: pd.DataFrame) -> None:
    """Check decoded labels against authoritative lookup tables."""
    for label_col, source_col in DECODED_LABEL_COLUMNS.items():
        lookup_spec = DECODED_LABEL_LOOKUPS.get(label_col)
        if lookup_spec is None:
            result.add(f"Decoded label correctness: {label_col}", False, "No authoritative lookup configured")
            continue
        if label_col not in df.columns:
            result.add(f"Decoded label correctness: {label_col}", False, "Column not found")
            continue
        if source_col not in df.columns:
            result.add(
                f"Decoded label correctness: {label_col}",
                False,
                f"Source column missing: {source_col}",
            )
            continue

        lookup, zero_pad = lookup_spec
        source_mask = df[source_col].notna()
        if not bool(source_mask.any()):
            result.add(f"Decoded label correctness: {label_col}", True, "No source-coded rows present")
            continue

        expected = df.loc[source_mask, source_col].map(
            lambda value: decode_lookup_value(value, lookup, zero_pad=zero_pad)
        )
        actual = df.loc[source_mask, label_col].map(
            lambda value: value.strip() if isinstance(value, str) else value
        )

        missing_lookup = int(expected.isna().sum())
        comparable = pd.DataFrame({"expected": expected, "actual": actual}).dropna(subset=["expected"])
        mismatch_count = int(comparable["expected"].ne(comparable["actual"]).sum())
        result.add(
            f"Decoded label correctness: {label_col}",
            missing_lookup == 0 and mismatch_count == 0,
            (
                f"{len(comparable) - mismatch_count:,}/{len(comparable):,} authoritative matches; "
                f"lookup_missing={missing_lookup:,}, mismatches={mismatch_count:,}"
            ),
        )


def validate_county_all_semantics(result: ValidationResult, df: pd.DataFrame) -> None:
    """Check county_all export structure and alignment with the major county."""
    if "county_all" not in df.columns:
        result.add("county_all column", False, "Column not found")
        return

    result.add("county_all column", True, "Column present")

    parsed_values = df["county_all"].map(split_county_all)
    valid_county_names = {
        str(county_name).strip().casefold()
        for county_name in COUNTY_CODE_LOOKUP.values()
        if str(county_name).strip()
    }
    blank_token_rows = int(df["county_all"].map(county_all_has_blank_token).sum())
    duplicate_rows = int(
        parsed_values.map(
            lambda values: len({value.casefold() for value in values}) != len(values)
        ).sum()
    )
    unknown_token_rows = int(
        parsed_values.map(
            lambda values: any(value.casefold() not in valid_county_names for value in values)
        ).sum()
    )
    county_name_missing_rows = 0
    leading_name_mismatch_rows = 0
    single_name_mismatch_rows = 0
    if "COUNTY_NAME" in df.columns:
        county_name_series = df["COUNTY_NAME"].map(clean_optional_text)
        county_name_missing_rows = int(
            sum(
                county_name is not None
                and county_name.casefold() not in {value.casefold() for value in values}
                for county_name, values in zip(
                    county_name_series.tolist(),
                    parsed_values.tolist(),
                )
            )
        )
        leading_name_mismatch_rows = int(
            sum(
                bool(values)
                and county_name is not None
                and values[0].casefold() != county_name.casefold()
                for county_name, values in zip(
                    county_name_series.tolist(),
                    parsed_values.tolist(),
                )
            )
        )
        single_name_mismatch_rows = int(
            sum(
                len(values) == 1
                and county_name is not None
                and values[0].casefold() != county_name.casefold()
                for county_name, values in zip(
                    county_name_series.tolist(),
                    parsed_values.tolist(),
                )
            )
        )

    result.add(
        "county_all token structure",
        blank_token_rows == 0 and duplicate_rows == 0 and unknown_token_rows == 0,
        (
            "No blank, duplicate, or unknown county tokens"
            if blank_token_rows == 0 and duplicate_rows == 0 and unknown_token_rows == 0
            else (
                f"blank_rows={blank_token_rows:,}, duplicate_rows={duplicate_rows:,}, "
                f"unknown_rows={unknown_token_rows:,}"
            )
        ),
    )
    result.add(
        "county_all contains COUNTY_NAME",
        county_name_missing_rows == 0,
        (
            "COUNTY_NAME is present in county_all for all populated rows"
            if county_name_missing_rows == 0
            else f"{county_name_missing_rows:,} rows missing COUNTY_NAME inside county_all"
        ),
    )
    result.add(
        "county_all leading COUNTY_NAME alignment",
        leading_name_mismatch_rows == 0,
        (
            "county_all starts with COUNTY_NAME for all populated rows"
            if leading_name_mismatch_rows == 0
            else f"{leading_name_mismatch_rows:,} rows start with a county other than COUNTY_NAME"
        ),
    )
    result.add(
        "county_all single-name alignment",
        single_name_mismatch_rows == 0,
        (
            "Single-name county_all rows align with COUNTY_NAME"
            if single_name_mismatch_rows == 0
            else f"{single_name_mismatch_rows:,} single-name rows disagree with COUNTY_NAME"
        ),
    )


def validate_phase1_attribute_columns(result: ValidationResult, df: pd.DataFrame) -> None:
    """Check that the expanded raw roadway attribute columns are staged."""
    for column in EXPECTED_PHASE1_ATTRIBUTE_COLUMNS:
        result.add(
            f"Phase 1 attribute: {column}",
            column in df.columns,
            "Column present" if column in df.columns else "Column not found",
        )


def validate_route_family_columns(result: ValidationResult, df: pd.DataFrame) -> None:
    """Check that the Georgia route-family crosswalk fields are staged."""
    for column in EXPECTED_ROUTE_FAMILY_COLUMNS:
        result.add(
            f"Route-family field: {column}",
            column in df.columns,
            "Column present" if column in df.columns else "Column not found",
        )


def validate_gdot_route_type_columns(result: ValidationResult, df: pd.DataFrame) -> None:
    """Check that the granular Georgia route-type fields are staged and populated."""
    for column in EXPECTED_GDOT_ROUTE_TYPE_COLUMNS:
        result.add(
            f"GDOT route-type field: {column}",
            column in df.columns,
            "Column present" if column in df.columns else "Column not found",
        )

    if "ROUTE_TYPE_GDOT" in df.columns:
        null_count = int(df["ROUTE_TYPE_GDOT"].isna().sum())
        result.add(
            "GDOT route-type coverage",
            null_count == 0,
            f"{len(df) - null_count:,}/{len(df):,} rows classified",
        )

    if "HWY_NAME" in df.columns:
        null_count = int(df["HWY_NAME"].isna().sum())
        result.add(
            "HWY_NAME coverage",
            null_count == 0,
            f"{len(df) - null_count:,}/{len(df):,} rows named",
        )


def validate_signed_route_verification_columns(result: ValidationResult, df: pd.DataFrame) -> None:
    """Check that signed-route verification fields are staged."""
    for column in EXPECTED_SIGNED_ROUTE_VERIFICATION_COLUMNS:
        result.add(
            f"Signed-route field: {column}",
            column in df.columns,
            "Column present" if column in df.columns else "Column not found",
        )


def validate_signed_route_family_slots(result: ValidationResult, df: pd.DataFrame) -> None:
    """Check signed-route family list/slot semantics for filtering and export."""
    required_columns = [
        "SIGNED_ROUTE_FAMILY_PRIMARY",
        "SECONDARY_SIGNED_ROUTE_FAMILY",
        "TERTIARY_SIGNED_ROUTE_FAMILY",
        "SIGNED_ROUTE_FAMILY_ALL",
    ]
    missing_columns = [column for column in required_columns if column not in df.columns]
    if missing_columns:
        result.add(
            "Signed-route family slot semantics",
            False,
            f"Missing columns: {', '.join(missing_columns)}",
        )
        return

    invalid_family_rows = 0
    slot_mismatch_rows = 0

    for row in df[required_columns].itertuples(index=False):
        primary, secondary, tertiary, all_value = row
        try:
            families = json.loads(all_value) if isinstance(all_value, str) and all_value.strip() else []
        except json.JSONDecodeError:
            slot_mismatch_rows += 1
            continue

        if not isinstance(families, list):
            slot_mismatch_rows += 1
            continue

        cleaned_families = [str(family).strip() for family in families if str(family).strip()]
        if any(family not in SIGNED_ROUTE_FAMILIES for family in cleaned_families):
            invalid_family_rows += 1

        expected_families = [
            family for family in cleaned_families if family in SIGNED_ROUTE_FAMILIES
        ]
        expected_primary = expected_families[0] if len(expected_families) > 0 else None
        expected_secondary = expected_families[1] if len(expected_families) > 1 else None
        expected_tertiary = expected_families[2] if len(expected_families) > 2 else None

        actual_primary = primary.strip() if isinstance(primary, str) else None
        actual_secondary = secondary.strip() if isinstance(secondary, str) else None
        actual_tertiary = tertiary.strip() if isinstance(tertiary, str) else None

        if expected_primary is not None:
            primary_ok = actual_primary == expected_primary
        else:
            primary_ok = actual_primary not in SIGNED_ROUTE_FAMILIES

        if not (
            primary_ok
            and actual_secondary == expected_secondary
            and actual_tertiary == expected_tertiary
        ):
            slot_mismatch_rows += 1

    result.add(
        "Signed-route family list semantics",
        invalid_family_rows == 0,
        (
            "All SIGNED_ROUTE_FAMILY_ALL entries contain only signed-route families"
            if invalid_family_rows == 0
            else f"{invalid_family_rows:,} rows contain non-signed families in SIGNED_ROUTE_FAMILY_ALL"
        ),
    )
    result.add(
        "Signed-route family slot alignment",
        slot_mismatch_rows == 0,
        (
            "PRIMARY/SECONDARY/TERTIARY align with SIGNED_ROUTE_FAMILY_ALL"
            if slot_mismatch_rows == 0
            else f"{slot_mismatch_rows:,} rows have misaligned signed-route family slots"
        ),
    )


def validate_current_aadt_provenance_columns(result: ValidationResult, df: pd.DataFrame) -> None:
    """Check that canonical 2024 AADT provenance fields are staged."""
    for column in EXPECTED_CURRENT_AADT_PROVENANCE_COLUMNS:
        result.add(
            f"Current AADT provenance field: {column}",
            column in df.columns,
            "Column present" if column in df.columns else "Column not found",
        )


def validate_provenance_consistency(result: ValidationResult, df: pd.DataFrame) -> None:
    """Check that populated values have matching provenance fields."""

    # Speed limit: every populated SPEED_LIMIT should have a source
    if "SPEED_LIMIT" in df.columns and "SPEED_LIMIT_SOURCE" in df.columns:
        has_speed = df["SPEED_LIMIT"].notna()
        has_source = df["SPEED_LIMIT_SOURCE"].notna()
        orphaned = int((has_speed & ~has_source).sum())
        result.add(
            "Speed limit provenance",
            orphaned == 0,
            f"{int(has_speed.sum()):,} with speed limit, {orphaned} missing source"
            if orphaned > 0
            else f"{int(has_speed.sum()):,} with speed limit, all have source",
        )

    # Future AADT: every populated FUTURE_AADT_2044 should have a source
    if "FUTURE_AADT_2044" in df.columns and "FUTURE_AADT_2044_SOURCE" in df.columns:
        has_val = df["FUTURE_AADT_2044"].notna()
        has_source = df["FUTURE_AADT_2044_SOURCE"].notna() & (df["FUTURE_AADT_2044_SOURCE"] != "missing")
        orphaned = int((has_val & ~has_source).sum())
        result.add(
            "Future AADT provenance",
            orphaned == 0,
            f"{int(has_val.sum()):,} with future AADT, {orphaned} missing source"
            if orphaned > 0
            else f"{int(has_val.sum()):,} with future AADT, all have source",
        )


def validate_current_aadt_audit_artifacts(result: ValidationResult) -> None:
    """Check that the retained current-year AADT audit summary was written."""
    exists = CURRENT_AADT_AUDIT_SUMMARY_ARTIFACT.exists()
    detail = (
        f"{CURRENT_AADT_AUDIT_SUMMARY_ARTIFACT.name} present"
        if exists
        else f"Missing: {CURRENT_AADT_AUDIT_SUMMARY_ARTIFACT}"
    )
    result.add(
        f"Current AADT audit artifact: {CURRENT_AADT_AUDIT_SUMMARY_ARTIFACT.name}",
        exists,
        detail,
    )


def cleanup_current_aadt_audit_artifacts() -> None:
    """Remove transient detailed AADT audit CSVs after validation finishes."""
    for path in CURRENT_AADT_AUDIT_TRANSIENT_ARTIFACTS:
        if path.exists():
            path.unlink()
            logger.info("Removed transient current AADT audit artifact: %s", path)
    if CURRENT_AADT_AUDIT_TMP_DIR.exists() and not any(CURRENT_AADT_AUDIT_TMP_DIR.iterdir()):
        CURRENT_AADT_AUDIT_TMP_DIR.rmdir()
        logger.info("Removed empty transient AADT audit directory: %s", CURRENT_AADT_AUDIT_TMP_DIR)


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
    result.add(
        "GeoPackage county_all column",
        "county_all" in gdf.columns,
        "county_all present" if "county_all" in gdf.columns else "Missing county_all in roadway_segments",
    )
    slot_columns_present = {
        "SECONDARY_SIGNED_ROUTE_FAMILY",
        "TERTIARY_SIGNED_ROUTE_FAMILY",
    }.issubset(gdf.columns)
    result.add(
        "GeoPackage signed-route slot columns",
        slot_columns_present,
        (
            "SECONDARY_SIGNED_ROUTE_FAMILY and TERTIARY_SIGNED_ROUTE_FAMILY present"
            if slot_columns_present
            else "Missing signed-route slot columns in roadway_segments"
        ),
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
    result.add(
        "County boundary geometry validity",
        bool(county_gdf.geometry.is_valid.all()),
        f"{int((~county_gdf.geometry.is_valid).sum()):,} invalid of {len(county_gdf):,}",
    )
    result.add(
        "District boundary geometry validity",
        bool(district_gdf.geometry.is_valid.all()),
        f"{int((~district_gdf.geometry.is_valid).sum()):,} invalid of {len(district_gdf):,}",
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
    if {"COUNTYFP", "NAME"}.issubset(county_gdf.columns):
        county_lookup = {
            str(county_code).zfill(3): str(county_name).strip()
            for county_code, county_name in county_gdf[["COUNTYFP", "NAME"]].itertuples(index=False)
        }
        mismatches = [
            county_code
            for county_code, county_name in COUNTY_CODE_LOOKUP.items()
            if county_lookup.get(county_code) != county_name
        ]
        result.add(
            "County boundary lookup correctness",
            len(county_lookup) == 159 and not mismatches,
            (
                f"{159 - len(mismatches):,}/159 county FIPS-name pairs match county_codes.json"
                if len(county_lookup) == 159
                else f"Expected 159 county polygons, found {len(county_lookup)}"
            ),
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

        # Check row count matches CSV and GeoPackage
        if "segments" in tables:
            db_count = conn.execute("SELECT COUNT(*) FROM segments").fetchone()[0]
            result.add(
                "Database row count",
                db_count >= MIN_ROW_COUNT,
                f"{db_count:,} rows; threshold >= {MIN_ROW_COUNT:,}",
            )

            segment_columns = {
                row[1]
                for row in conn.execute("PRAGMA table_info(segments)").fetchall()
            }
            result.add(
                "Database column count",
                len(segment_columns) >= MIN_COLUMN_COUNT,
                f"{len(segment_columns):,} columns; threshold >= {MIN_COLUMN_COUNT}",
            )
            slot_columns_present = {
                "SECONDARY_SIGNED_ROUTE_FAMILY",
                "TERTIARY_SIGNED_ROUTE_FAMILY",
            }.issubset(segment_columns)
            result.add(
                "Database signed-route slot columns",
                slot_columns_present,
                (
                    "SECONDARY_SIGNED_ROUTE_FAMILY and TERTIARY_SIGNED_ROUTE_FAMILY present"
                    if slot_columns_present
                    else "Missing signed-route slot columns in segments table"
                ),
            )
            result.add(
                "Database county_all column",
                "county_all" in segment_columns,
                "county_all present" if "county_all" in segment_columns else "Missing county_all in segments table",
            )

            csv_path = TABLES_DIR / "roadway_inventory_cleaned.csv"
            if csv_path.exists():
                csv_count = sum(1 for _ in open(csv_path, encoding="utf-8")) - 1
                match = db_count == csv_count
                result.add(
                    "DB/CSV row count match",
                    match,
                    f"DB={db_count:,}, CSV={csv_count:,}" + ("" if match else " — MISMATCH"),
                )

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
        statewide_count = len(statewide.Roadway_Inventory) if statewide.Roadway_Inventory is not None else 0

        district = RoadwayData(district_id=7)
        district.load_data()
        district_count = len(district.Roadway_Inventory) if district.Roadway_Inventory is not None else 0
        district_values = (
            set(pd.to_numeric(district.Roadway_Inventory["DISTRICT"], errors="coerce").dropna().astype(int).unique())
            if district.Roadway_Inventory is not None and "DISTRICT" in district.Roadway_Inventory.columns
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
    try:
        # Run checks
        df = validate_row_count(result)

        if df is not None:
            validate_column_count(result, df)
            validate_unique_id(result, df)
            validate_null_checks(result, df)
            validate_district_range(result, df)
            validate_aadt_coverage(result, df)
            validate_state_system_location_fields(result, df)
            validate_phase1_attribute_columns(result, df)
            validate_route_family_columns(result, df)
            validate_gdot_route_type_columns(result, df)
            validate_signed_route_verification_columns(result, df)
            validate_signed_route_family_slots(result, df)
            validate_current_aadt_provenance_columns(result, df)
            validate_provenance_consistency(result, df)
            validate_decoded_labels(result, df)
            validate_decoded_label_correctness(result, df)
            validate_county_all_semantics(result, df)

        validate_crs(result)
        validate_geometry(result)
        validate_boundary_layers(result)
        validate_database(result)
        validate_current_aadt_audit_artifacts(result)
        validate_raptor_loader(result)

        # Summary
        logger.info("")
        logger.info(
            "Validation %s: %s",
            "PASSED" if result.all_passed else "FAILED",
            result.summary(),
        )

        # Write results to JSON
        output_path = PROJECT_ROOT / "02-Data-Staging" / "reports" / "validation_results.json"
        output_path.write_text(json.dumps(result.checks, indent=2))
        logger.info("Results written to %s", output_path)

        if not result.all_passed:
            raise SystemExit(1)
    finally:
        cleanup_current_aadt_audit_artifacts()


if __name__ == "__main__":
    main()
