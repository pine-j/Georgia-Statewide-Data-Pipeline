"""Create station_time_series.gpkg — wide-format AADT + truck by station UID.

Outputs one row per resolved station_uid with per-year AADT columns,
truck AADT columns, and Point geometry in EPSG:4326.
"""

from __future__ import annotations

import logging
import sqlite3
import sys
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from scipy.spatial import cKDTree
from shapely.geometry import Point

_SCRIPTS = Path(__file__).resolve().parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from segment_station_link import _project_lon_lat_to_metric

logger = logging.getLogger(__name__)

DB_PATH = _SCRIPTS.parents[1] / "databases" / "roadway_inventory.db"
OUTPUT_PATH = _SCRIPTS.parents[2] / "03-Processed-Data" / "aadt" / "station_time_series.gpkg"

YEARS = list(range(2015, 2025))


def _find_segment_future_col(conn: sqlite3.Connection) -> str | None:
    segment_cols = {row[1] for row in conn.execute("PRAGMA table_info(segments)").fetchall()}
    for candidate in ("FUTURE_AADT_2044", "FUTURE_AADT", "Future_AADT"):
        if candidate in segment_cols:
            return candidate
    return None


def _attach_future_aadt(conn: sqlite3.Connection, wide: pd.DataFrame) -> pd.DataFrame:
    wide = wide.copy()
    wide["FUTURE_AADT_2044"] = np.nan

    future_col = _find_segment_future_col(conn)
    if future_col is None or wide.empty:
        return wide

    segs = pd.read_sql(
        f"SELECT latitude, longitude, {future_col} AS future_aadt FROM segments "
        f"WHERE latitude IS NOT NULL AND longitude IS NOT NULL AND {future_col} IS NOT NULL",
        conn,
    )
    if segs.empty:
        return wide

    valid_mask = wide["latitude"].notna() & wide["longitude"].notna()
    if not valid_mask.any():
        return wide

    seg_x, seg_y = _project_lon_lat_to_metric(
        segs["longitude"].to_numpy(), segs["latitude"].to_numpy()
    )
    tree = cKDTree(np.c_[seg_x, seg_y])

    station_x, station_y = _project_lon_lat_to_metric(
        wide.loc[valid_mask, "longitude"].to_numpy(),
        wide.loc[valid_mask, "latitude"].to_numpy(),
    )
    _, nearest_idx = tree.query(np.c_[station_x, station_y], k=1)

    future_vals = pd.to_numeric(segs["future_aadt"], errors="coerce").to_numpy(dtype="float64")
    wide.loc[valid_mask, "FUTURE_AADT_2044"] = future_vals[np.asarray(nearest_idx, dtype=int)]
    return wide


def build_station_wide(
    conn: sqlite3.Connection,
    years: list[int] | None = None,
) -> gpd.GeoDataFrame:
    """Build wide-format GeoDataFrame of station time series."""
    if years is None:
        years = YEARS

    resolver = pd.read_sql(
        "SELECT year, tc_number, station_uid, resolver_method, resolver_confidence "
        "FROM station_uid_resolver WHERE resolver_method != 'unresolved'",
        conn,
    )

    stations = pd.read_sql(
        "SELECT year, tc_number, latitude, longitude, aadt, "
        "single_unit_aadt, combo_unit_aadt, functional_class, "
        "statistics_type, future_aadt "
        "FROM historic_stations",
        conn,
    )

    merged = resolver.merge(stations, on=["year", "tc_number"], how="left")
    merged["truck_aadt"] = merged.apply(
        lambda r: (
            (r["single_unit_aadt"] if pd.notna(r["single_unit_aadt"]) else 0)
            + (r["combo_unit_aadt"] if pd.notna(r["combo_unit_aadt"]) else 0)
        )
        if pd.notna(r["single_unit_aadt"]) or pd.notna(r["combo_unit_aadt"])
        else np.nan,
        axis=1,
    )

    anchor_year = max(years)
    anchor = merged[merged["year"] == anchor_year].drop_duplicates(
        subset=["station_uid"], keep="first"
    )
    uid_coords = anchor[["station_uid", "latitude", "longitude", "functional_class",
                         "statistics_type"]].copy()
    uid_coords = uid_coords.rename(columns={
        "functional_class": "functional_class",
        "statistics_type": f"statistics_type_{anchor_year}",
    })

    aadt_pivot = merged.pivot_table(
        index="station_uid", columns="year", values="aadt", aggfunc="first"
    )
    truck_pivot = merged.pivot_table(
        index="station_uid", columns="year", values="truck_aadt", aggfunc="first"
    )

    for y in years:
        if y not in aadt_pivot.columns:
            aadt_pivot[y] = np.nan
        if y not in truck_pivot.columns:
            truck_pivot[y] = np.nan

    aadt_pivot = aadt_pivot.rename(columns={y: f"AADT_{y}" for y in years})
    truck_pivot = truck_pivot.rename(columns={y: f"TRUCK_AADT_{y}" for y in years})

    aadt_cols = [f"AADT_{y}" for y in years]
    wide = uid_coords.merge(aadt_pivot[aadt_cols], left_on="station_uid", right_index=True, how="left")
    truck_cols = [f"TRUCK_AADT_{y}" for y in years]
    wide = wide.merge(truck_pivot[truck_cols], left_on="station_uid", right_index=True, how="left")

    wide["years_with_data"] = wide[aadt_cols].notna().sum(axis=1).astype(int)

    tc_2024 = merged[merged["year"] == anchor_year][["station_uid", "tc_number"]].drop_duplicates(
        subset=["station_uid"], keep="first"
    ).rename(columns={"tc_number": f"tc_number_{anchor_year}"})
    wide = wide.merge(tc_2024, on="station_uid", how="left")
    wide = _attach_future_aadt(conn, wide)

    geometry = [
        Point(lon, lat) if pd.notna(lat) and pd.notna(lon) else None
        for lat, lon in zip(wide["latitude"], wide["longitude"])
    ]
    gdf = gpd.GeoDataFrame(wide, geometry=geometry, crs="EPSG:4326")

    return gdf


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    conn = sqlite3.connect(str(DB_PATH))

    logger.info("Building station time series wide table...")
    gdf = build_station_wide(conn)
    logger.info("  %d stations, %d columns", len(gdf), len(gdf.columns))
    logger.info("  years_with_data distribution: %s",
                gdf["years_with_data"].value_counts().sort_index().to_dict())

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_file(str(OUTPUT_PATH), driver="GPKG", layer="aadt_stations")
    logger.info("Wrote %s", OUTPUT_PATH)

    conn.close()
    logger.info("Done.")


if __name__ == "__main__":
    main()
