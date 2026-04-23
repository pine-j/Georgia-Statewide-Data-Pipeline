"""TDD tests for apply_station_uid_resolver (scored resolver integration).

Tests verify:
1. Resolver writes table with correct schema
2. Anchor (2024) rows get resolver_method="anchor"
3. Confidence mapping from score_margin
4. All years present in output
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pandas as pd
import pytest

_SCRIPTS = Path(__file__).resolve().parents[2] / "scripts" / "07_aadt_historic_model"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from apply_station_uid_resolver import (
    ANCHOR_YEAR,
    RESOLVER_TABLE,
    map_confidence,
    run_resolver,
)


def _make_db(tmp_path: Path, years: list[int]) -> Path:
    """Create a test SQLite DB with historic_stations data."""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))

    stations = []
    base_lat, base_lon = 33.5, -84.5
    for year in years:
        for i in range(5):
            tc = f"TC{i:03d}"
            stations.append({
                "year": year,
                "tc_number": tc,
                "latitude": base_lat + i * 0.001,
                "longitude": base_lon + i * 0.001,
                "aadt": 5000 + i * 1000,
                "functional_class": 3,
                "statistics_type": "Actual",
                "single_unit_aadt": 200 + i * 50,
                "combo_unit_aadt": 300 + i * 50,
                "k_factor": 0.085,
                "d_factor": 0.52,
                "station_type": "Short Term",
                "traffic_class": None,
                "future_aadt": 10000 + i * 2000,
                "source": f"test:{year}",
            })

    df = pd.DataFrame(stations)
    df.to_sql("historic_stations", conn, if_exists="replace", index=False)
    conn.commit()
    conn.close()
    return db_path


class TestApplyResolver:
    def test_apply_resolver_writes_table(self, tmp_path):
        db_path = _make_db(tmp_path, [2020, 2021, 2024])
        conn = sqlite3.connect(str(db_path))
        run_resolver(conn)

        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        assert RESOLVER_TABLE in tables

        cols = [r[1] for r in conn.execute(
            f"PRAGMA table_info({RESOLVER_TABLE})"
        ).fetchall()]
        expected = {"year", "tc_number", "station_uid", "resolver_method",
                    "resolver_delta_m", "resolver_confidence"}
        assert expected.issubset(set(cols))
        conn.close()

    def test_apply_resolver_anchor_rows(self, tmp_path):
        db_path = _make_db(tmp_path, [2020, 2024])
        conn = sqlite3.connect(str(db_path))
        run_resolver(conn)

        anchor_rows = pd.read_sql(
            f"SELECT * FROM {RESOLVER_TABLE} WHERE year = {ANCHOR_YEAR}", conn
        )
        assert len(anchor_rows) == 5
        assert (anchor_rows["resolver_method"] == "anchor").all()
        assert (anchor_rows["resolver_confidence"] == "high").all()
        conn.close()

    def test_apply_resolver_confidence_mapping(self, tmp_path):
        assert map_confidence(0.15) == "high"
        assert map_confidence(0.10001) == "high"
        assert map_confidence(0.08) == "medium"
        assert map_confidence(0.05001) == "medium"
        assert map_confidence(0.03) == "low"
        assert map_confidence(0.0) == "low"
        assert map_confidence(float("nan")) == "low"

    def test_apply_resolver_all_years_present(self, tmp_path):
        years = [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024]
        db_path = _make_db(tmp_path, years)
        conn = sqlite3.connect(str(db_path))
        run_resolver(conn)

        result_years = {r[0] for r in conn.execute(
            f"SELECT DISTINCT year FROM {RESOLVER_TABLE}"
        ).fetchall()}
        assert result_years == set(years)

        total = conn.execute(f"SELECT COUNT(*) FROM {RESOLVER_TABLE}").fetchone()[0]
        assert total == len(years) * 5
        conn.close()
