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
from shapely.geometry import Point

_SCRIPTS = Path(__file__).resolve().parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

logger = logging.getLogger(__name__)

DB_PATH = _SCRIPTS.parents[1] / "databases" / "roadway_inventory.db"
OUTPUT_PATH = _SCRIPTS.parents[2] / "03-Processed-Data" / "aadt" / "station_time_series.gpkg"

YEARS = list(range(2015, 2025))


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

    try:
        segs = pd.read_sql(
            "SELECT latitude, longitude, Future_AADT FROM segments "
            "WHERE latitude IS NOT NULL AND longitude IS NOT NULL AND Future_AADT IS NOT NULL",
            conn,
        )
        if not segs.empty and not wide.empty:
            from scored_resolver import haversine_m
            future_aadts = []
            seg_lats = segs["latitude"].values
            seg_lons = segs["longitude"].values
            seg_fa = segs["Future_AADT"].values
            for _, row in wide.iterrows():
                if pd.notna(row["latitude"]) and pd.notna(row["longitude"]):
                    dists = haversine_m(row["latitude"], row["longitude"], seg_lats, seg_lons)
                    nearest_idx = np.argmin(dists)
                    future_aadts.append(int(seg_fa[nearest_idx]))
                else:
                    future_aadts.append(np.nan)
            wide["FUTURE_AADT_2044"] = future_aadts
        else:
            wide["FUTURE_AADT_2044"] = np.nan
    except Exception:
        wide["FUTURE_AADT_2044"] = np.nan

    geometry = [
        Point(lon, lat) if pd.notna(lat) and pd.notna(lon) else Point(0, 0)
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
