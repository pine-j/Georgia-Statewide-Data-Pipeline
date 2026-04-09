"""Normalize socioeconomic datasets for the Georgia pipeline.

This script:
1. Standardizes FIPS codes across all datasets (zero-padded strings)
2. Joins ACS tabular data to TIGER block-group geometries
3. Cleans and validates fields (numeric coercion, null handling)
4. Outputs normalized files to ``02-Data-Staging/tables/demographics/``
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import geopandas as gpd
import pandas as pd

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[3]
RAW_DIR = REPO_ROOT / "01-Raw-Data" / "demographics"
CLEAN_DIR = REPO_ROOT / "02-Data-Staging" / "tables" / "demographics"
CONFIG_DIR = REPO_ROOT / "02-Data-Staging" / "config"

STATE_FIPS = "13"


# ---------------------------------------------------------------------------
# FIPS helpers
# ---------------------------------------------------------------------------

def _pad_fips(df: pd.DataFrame, col: str, width: int) -> pd.DataFrame:
    """Zero-pad a FIPS column to the expected width."""
    df[col] = df[col].astype(str).str.zfill(width)
    return df


def _build_block_group_geoid(row: pd.Series) -> str:
    """Construct a 12-digit block-group GEOID from component columns."""
    return (
        str(row.get("state", "")).zfill(2)
        + str(row.get("county", "")).zfill(3)
        + str(row.get("tract", "")).zfill(6)
        + str(row.get("block group", row.get("block_group", ""))).zfill(1)
    )


def _normalize_county_name(value: str) -> str:
    return "".join(character for character in str(value).strip().upper() if character.isalnum())


def _load_county_fips_lookup() -> dict[str, str]:
    county_codes = json.loads((CONFIG_DIR / "county_codes.json").read_text(encoding="utf-8"))
    return {
        _normalize_county_name(county_name): county_fips
        for county_fips, county_name in county_codes.items()
    }


# ---------------------------------------------------------------------------
# Normalize functions
# ---------------------------------------------------------------------------

def normalize_decennial(state_fips: str = STATE_FIPS) -> pd.DataFrame:
    """Normalize Decennial 2020 block-level data."""
    pattern = list((RAW_DIR / "census_decennial").glob(f"decennial_*_{state_fips}.csv"))
    if not pattern:
        logger.warning("No decennial CSV found for state %s", state_fips)
        return pd.DataFrame()

    df = pd.read_csv(pattern[0], dtype=str)

    # Ensure GEOID is present and zero-padded (15-digit block GEOID)
    if "GEOID" not in df.columns:
        df["GEOID"] = (
            df["state"].str.zfill(2)
            + df["county"].str.zfill(3)
            + df["tract"].str.zfill(6)
            + df["block"].str.zfill(4)
        )
    df["GEOID"] = df["GEOID"].str.zfill(15)

    # Numeric coercion
    numeric_cols = [c for c in df.columns if c.startswith(("P1_", "H1_"))]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    # Derived: county FIPS for joins
    df["COUNTY_FIPS"] = df["GEOID"].str[:5]

    logger.info("Decennial: %d blocks normalized", len(df))
    return df


def normalize_acs(state_fips: str = STATE_FIPS) -> gpd.GeoDataFrame:
    """Normalize ACS 5-Year block-group data and join to TIGER geometries."""
    # Load tabular ACS data
    acs_files = list((RAW_DIR / "acs_5year").glob(f"acs5_*_{state_fips}.csv"))
    if not acs_files:
        logger.warning("No ACS CSV found for state %s", state_fips)
        return gpd.GeoDataFrame()

    acs = pd.read_csv(acs_files[0], dtype=str)

    if "GEOID" not in acs.columns:
        acs["GEOID"] = acs.apply(_build_block_group_geoid, axis=1)
    acs["GEOID"] = acs["GEOID"].str.zfill(12)

    # Numeric coercion for all B-series variables
    b_cols = [c for c in acs.columns if c.startswith("B")]
    for col in b_cols:
        acs[col] = pd.to_numeric(acs[col], errors="coerce")

    # Compute derived fields
    acs["pct_poverty"] = (acs.get("B17001_002E", 0) / acs.get("B17001_001E", 1)).fillna(0)
    acs["pct_bachelors_plus"] = (
        (acs.get("B15003_022E", 0) + acs.get("B15003_023E", 0) + acs.get("B15003_025E", 0))
        / acs.get("B15003_001E", 1)
    ).fillna(0)
    acs["pct_transit"] = (acs.get("B08301_010E", 0) / acs.get("B08301_001E", 1)).fillna(0)

    # County FIPS for convenience
    acs["COUNTY_FIPS"] = acs["GEOID"].str[:5]

    # Load TIGER block-group shapefile
    bg_dirs = list((RAW_DIR / "tiger_shapefiles" / "block_groups").glob("*.shp"))
    if bg_dirs:
        tiger_bg = gpd.read_file(bg_dirs[0])
        tiger_bg = tiger_bg.to_crs(epsg=32617)
        tiger_bg["GEOID"] = tiger_bg["GEOID"].str.zfill(12)

        # Join tabular to geometry
        gdf = tiger_bg.merge(acs.drop(columns=["geometry"], errors="ignore"), on="GEOID", how="left")
        logger.info("ACS joined to TIGER: %d block groups", len(gdf))
        return gdf

    logger.warning("No TIGER block-group shapefile found; returning tabular only")
    return gpd.GeoDataFrame(acs)


def normalize_lodes() -> dict[str, pd.DataFrame]:
    """Normalize LEHD LODES WAC and RAC files."""
    results: dict[str, pd.DataFrame] = {}

    for kind in ("wac", "rac"):
        files = list((RAW_DIR / "lehd_lodes").glob(f"*_{kind}_*.csv"))
        if not files:
            logger.warning("No LODES %s CSV found", kind.upper())
            continue

        df = pd.read_csv(files[0], dtype={"w_geocode": str, "h_geocode": str})

        # The geocode column name differs: w_geocode for WAC, h_geocode for RAC
        geocode_col = "w_geocode" if kind == "wac" else "h_geocode"
        if geocode_col in df.columns:
            df[geocode_col] = df[geocode_col].str.zfill(15)
            df["COUNTY_FIPS"] = df[geocode_col].str[:5]
            df["BLOCK_GROUP_GEOID"] = df[geocode_col].str[:12]

        results[kind] = df
        logger.info("LODES %s: %d rows normalized", kind.upper(), len(df))

    return results


def normalize_economic_census(state_fips: str = STATE_FIPS) -> pd.DataFrame:
    """Normalize Economic Census data."""
    files = list((RAW_DIR / "economic_census").glob(f"economic_census_*_{state_fips}.csv"))
    if not files:
        logger.warning("No Economic Census CSV found")
        return pd.DataFrame()

    df = pd.read_csv(files[0], dtype=str)

    for col in ("ESTAB", "PAYANN", "EMP"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "state" in df.columns and "county" in df.columns:
        df["COUNTY_FIPS"] = df["state"].str.zfill(2) + df["county"].str.zfill(3)

    logger.info("Economic Census: %d rows normalized", len(df))
    return df


def normalize_opb() -> pd.DataFrame:
    """Normalize OPB population projections."""
    files = list((RAW_DIR / "opb_projections").glob("*.xlsx"))
    if not files:
        logger.warning("No OPB projections Excel file found")
        return pd.DataFrame()

    raw = pd.read_excel(files[0], sheet_name=0, header=None)
    header_mask = raw.iloc[:, 0].astype(str).str.strip().eq("COUNTY")
    if not bool(header_mask.any()):
        raise AssertionError("OPB projections sheet is missing the COUNTY header row")

    header_index = int(raw.index[header_mask][0])
    raw_header = raw.iloc[header_index].tolist()
    normalized_header: list[str] = []
    for column_index, value in enumerate(raw_header):
        if column_index == 0:
            normalized_header.append("COUNTY")
            continue
        if pd.notna(value) and str(value).strip():
            try:
                normalized_header.append(str(int(float(value))))
            except (TypeError, ValueError):
                normalized_header.append(str(value).strip())
        else:
            normalized_header.append(f"unnamed_{column_index}")

    df = raw.iloc[header_index + 1:].copy()
    df.columns = normalized_header
    df = df[df["COUNTY"].notna()].copy()
    df["COUNTY"] = df["COUNTY"].astype(str).str.strip()
    df = df[
        ~df["COUNTY"].str.upper().eq("GEORGIA")
        & ~df["COUNTY"].str.startswith("Source:", na=False)
    ].copy()

    year_cols = [column for column in df.columns if column.isdigit()]
    df = df[["COUNTY", *year_cols]].copy()
    for column in year_cols:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    county_lookup = _load_county_fips_lookup()
    df["COUNTY_FIPS"] = df["COUNTY"].map(
        lambda county_name: county_lookup.get(_normalize_county_name(county_name))
    )
    mapped_count = int(df["COUNTY_FIPS"].notna().sum())
    unique_count = int(df["COUNTY_FIPS"].dropna().nunique())
    if mapped_count != 159 or unique_count != 159:
        missing = sorted(
            county_name
            for county_name in df.loc[df["COUNTY_FIPS"].isna(), "COUNTY"].astype(str).unique()
        )
        raise AssertionError(
            "OPB county-to-FIPS mapping must cover all 159 Georgia counties; "
            f"mapped_rows={mapped_count}, unique_fips={unique_count}, missing={missing[:10]}"
        )

    df = df.sort_values("COUNTY_FIPS").reset_index(drop=True)

    logger.info("OPB projections: %d rows normalized", len(df))
    return df


def normalize_opportunity_zones(state_fips: str = STATE_FIPS) -> pd.DataFrame:
    """Normalize Qualified Opportunity Zone tract list."""
    csv_files = list((RAW_DIR / "opportunity_zones").glob("*.csv"))
    if not csv_files:
        logger.warning("No QOZ CSV found")
        return pd.DataFrame()

    df = pd.read_csv(csv_files[0], dtype=str)

    # Filter to state
    tract_col = None
    for candidate in ("Census Tract Number", "GEOID", "tract"):
        if candidate in df.columns:
            tract_col = candidate
            break

    if tract_col:
        df[tract_col] = df[tract_col].str.zfill(11)
        df = df[df[tract_col].str[:2] == state_fips].copy()
        df = df.rename(columns={tract_col: "TRACT_GEOID"})
        df["COUNTY_FIPS"] = df["TRACT_GEOID"].str[:5]

    logger.info("Opportunity Zones: %d tracts in state %s", len(df), state_fips)
    return df


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def main(state_fips: str = STATE_FIPS) -> None:
    """Run the full normalization pipeline and save outputs."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)

    # Decennial
    decennial = normalize_decennial(state_fips)
    if not decennial.empty:
        decennial.to_csv(CLEAN_DIR / "decennial_blocks.csv", index=False)

    # ACS + TIGER join
    acs_gdf = normalize_acs(state_fips)
    if not acs_gdf.empty:
        if hasattr(acs_gdf, "geometry") and acs_gdf.geometry is not None:
            acs_gdf.to_file(CLEAN_DIR / "acs_block_groups.gpkg", driver="GPKG")
        else:
            acs_gdf.to_csv(CLEAN_DIR / "acs_block_groups.csv", index=False)

    # LODES
    lodes = normalize_lodes()
    for kind, df in lodes.items():
        df.to_csv(CLEAN_DIR / f"lodes_{kind}.csv", index=False)

    # Economic Census
    econ = normalize_economic_census(state_fips)
    if not econ.empty:
        econ.to_csv(CLEAN_DIR / "economic_census.csv", index=False)

    # OPB
    opb = normalize_opb()
    if not opb.empty:
        opb.to_csv(CLEAN_DIR / "opb_projections.csv", index=False)

    # Opportunity Zones
    oz = normalize_opportunity_zones(state_fips)
    if not oz.empty:
        oz.to_csv(CLEAN_DIR / "opportunity_zones.csv", index=False)

    logger.info("Normalization complete. Outputs in %s", CLEAN_DIR)


if __name__ == "__main__":
    main()
