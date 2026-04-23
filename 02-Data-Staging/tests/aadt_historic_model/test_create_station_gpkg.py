"""TDD tests for create_station_gpkg (station time series GPKG writer).

Tests cover:
1. Long-to-wide pivot produces correct AADT_{year} columns
2. Truck AADT columns present and equal SU+CU sum
3. Missing years produce NULL in that column
4. Output has Point geometry with correct CRS
5. years_with_data matches non-null AADT count
6. FUTURE_AADT_2044 populated from nearest segment
7. Only resolved UIDs appear in output
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

_SCRIPTS = Path(__file__).resolve().parents[2] / "scripts" / "07_aadt_historic_model"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from create_station_gpkg import (
    build_station_wide,
    YEARS,
)


def _make_test_db(tmp_path: Path) -> Path:
    """Create test DB with historic_stations + station_uid_resolver + segments."""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))

    hs_rows = []
    for year in [2020, 2021, 2022]:
        for i, tc in enumerate(["TC001", "TC002", "TC003"]):
            aadt = 5000 + i * 1000 + (year - 2020) * 100
            su = 200 + i * 50 if year != 2021 or tc != "TC003" else None
            cu = 300 + i * 50 if year != 2021 or tc != "TC003" else None
            hs_rows.append({
                "year": year,
                "tc_number": tc,
                "latitude": 33.5 + i * 0.01,
                "longitude": -84.5 + i * 0.01,
                "aadt": aadt,
                "statistics_type": "Actual",
                "single_unit_aadt": su,
                "combo_unit_aadt": cu,
                "k_factor": 0.085,
                "d_factor": 0.52,
                "functional_class": 3,
                "station_type": "Short Term",
                "traffic_class": None,
                "future_aadt": aadt * 2,
                "source": f"test:{year}",
            })
    pd.DataFrame(hs_rows).to_sql("historic_stations", conn, if_exists="replace", index=False)

    resolver_rows = []
    for year in [2020, 2021, 2022]:
        for tc in ["TC001", "TC002", "TC003"]:
            resolver_rows.append({
                "year": year,
                "tc_number": tc,
                "station_uid": f"GA24_{tc}",
                "resolver_method": "anchor" if year == 2022 else "tc_number",
                "resolver_delta_m": 0.0,
                "resolver_confidence": "high",
            })
    # Add an unresolved station
    resolver_rows.append({
        "year": 2020,
        "tc_number": "UNRESOLVED1",
        "station_uid": "GA20_UNRESOLVED1",
        "resolver_method": "unresolved",
        "resolver_delta_m": 999.0,
        "resolver_confidence": "low",
    })
    pd.DataFrame(resolver_rows).to_sql("station_uid_resolver", conn, if_exists="replace", index=False)

    seg_rows = []
    for i in range(5):
        seg_rows.append({
            "unique_id": f"SEG{i:03d}",
            "ROUTE_ID": f"GA{i:03d}",
            "AADT": 10000 + i * 1000,
            "latitude": 33.5 + i * 0.01,
            "longitude": -84.5 + i * 0.01,
            "FUTURE_AADT_2044": 20000 + i * 2000,
        })
    pd.DataFrame(seg_rows).to_sql("segments", conn, if_exists="replace", index=False)

    conn.commit()
    conn.close()
    return db_path


class TestStationGpkg:
    def test_pivot_long_to_wide(self, tmp_path):
        db_path = _make_test_db(tmp_path)
        conn = sqlite3.connect(str(db_path))
        wide = build_station_wide(conn, years=[2020, 2021, 2022])
        conn.close()

        assert "AADT_2020" in wide.columns
        assert "AADT_2021" in wide.columns
        assert "AADT_2022" in wide.columns
        assert len(wide) == 3  # 3 resolved UIDs

    def test_pivot_includes_truck_aadt(self, tmp_path):
        db_path = _make_test_db(tmp_path)
        conn = sqlite3.connect(str(db_path))
        wide = build_station_wide(conn, years=[2020, 2021, 2022])
        conn.close()

        assert "TRUCK_AADT_2020" in wide.columns
        assert "TRUCK_AADT_2021" in wide.columns
        row = wide[wide["station_uid"] == "GA24_TC001"].iloc[0]
        assert row["TRUCK_AADT_2020"] == 200 + 300  # SU + CU

    def test_pivot_handles_missing_year(self, tmp_path):
        db_path = _make_test_db(tmp_path)
        conn = sqlite3.connect(str(db_path))
        wide = build_station_wide(conn, years=[2019, 2020, 2021, 2022])
        conn.close()

        assert "AADT_2019" in wide.columns
        assert wide["AADT_2019"].isna().all()

    def test_gpkg_has_point_geometry(self, tmp_path):
        db_path = _make_test_db(tmp_path)
        conn = sqlite3.connect(str(db_path))
        wide = build_station_wide(conn, years=[2020, 2021, 2022])
        conn.close()

        assert "geometry" in wide.columns
        assert wide.geometry.geom_type.unique().tolist() == ["Point"]
        assert wide.crs.to_epsg() == 4326

    def test_years_with_data_count(self, tmp_path):
        db_path = _make_test_db(tmp_path)
        conn = sqlite3.connect(str(db_path))
        wide = build_station_wide(conn, years=[2020, 2021, 2022])
        conn.close()

        row = wide[wide["station_uid"] == "GA24_TC001"].iloc[0]
        assert row["years_with_data"] == 3  # all 3 years have AADT

    def test_future_aadt_attached(self, tmp_path):
        db_path = _make_test_db(tmp_path)
        conn = sqlite3.connect(str(db_path))
        wide = build_station_wide(conn, years=[2020, 2021, 2022])
        conn.close()

        assert "FUTURE_AADT_2044" in wide.columns
        assert wide["FUTURE_AADT_2044"].notna().any()

    def test_unresolved_stations_excluded(self, tmp_path):
        db_path = _make_test_db(tmp_path)
        conn = sqlite3.connect(str(db_path))
        wide = build_station_wide(conn, years=[2020, 2021, 2022])
        conn.close()

        assert "GA20_UNRESOLVED1" not in wide["station_uid"].values
