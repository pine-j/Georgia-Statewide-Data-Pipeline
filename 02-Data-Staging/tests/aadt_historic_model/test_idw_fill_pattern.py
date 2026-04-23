"""Integration tests for fill orchestrator patterns.

Tests the IDW fill workflow end-to-end using an in-memory SQLite DB:
  - Zero-target segments produces no crash
  - Rerun idempotence (same result on double-run)
  - Row count preservation (segments table unchanged)
"""

from __future__ import annotations

import sqlite3

import numpy as np
import pandas as pd
import pytest

from idw_predictor import predict_idw


def _setup_db() -> sqlite3.Connection:
    """Create an in-memory DB with minimal segments + stations + knn tables."""
    conn = sqlite3.connect(":memory:")
    conn.execute("""
        CREATE TABLE segments (
            unique_id TEXT PRIMARY KEY,
            FUNCTIONAL_CLASS REAL,
            AADT_2022_HPMS REAL,
            AADT_2022_HPMS_SYNTHETIC INTEGER
        )
    """)
    conn.execute("""
        CREATE TABLE historic_stations (
            year INTEGER, tc_number TEXT, aadt REAL,
            latitude REAL, longitude REAL, statistics_type TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE segment_station_link_knn (
            unique_id TEXT, year INTEGER, k_rank INTEGER,
            nearest_tc_number TEXT, station_distance_m REAL, same_route_flag INTEGER
        )
    """)

    conn.executemany("INSERT INTO segments VALUES (?, ?, ?, ?)", [
        ("seg_0", 7.0, None, None),
        ("seg_1", 7.0, None, None),
        ("seg_2", 4.0, 5000.0, 0),
    ])
    conn.executemany("INSERT INTO historic_stations VALUES (?, ?, ?, ?, ?, ?)", [
        (2022, "TC1", 3000, 33.5, -84.5, "Actual"),
        (2022, "TC2", 6000, 33.6, -84.5, "Estimated"),
    ])
    conn.executemany("INSERT INTO segment_station_link_knn VALUES (?, ?, ?, ?, ?, ?)", [
        ("seg_0", 2022, 1, "TC1", 200.0, 1),
        ("seg_0", 2022, 2, "TC2", 800.0, 0),
        ("seg_1", 2022, 1, "TC2", 500.0, 0),
        ("seg_2", 2022, 1, "TC1", 100.0, 1),
    ])
    conn.commit()
    return conn


def test_idw_on_target_segments_only() -> None:
    """IDW should only predict for the segments we pass in the knn."""
    conn = _setup_db()
    knn = pd.read_sql(
        "SELECT * FROM segment_station_link_knn WHERE unique_id IN ('seg_0', 'seg_1')",
        conn,
    )
    stations = pd.read_sql("SELECT tc_number, aadt FROM historic_stations WHERE year = 2022", conn)
    result = predict_idw(knn, stations)
    assert len(result) == 2
    assert set(result["unique_id"]) == {"seg_0", "seg_1"}
    conn.close()


def test_idw_rerun_idempotent() -> None:
    """Running IDW twice on the same input produces identical results."""
    conn = _setup_db()
    knn = pd.read_sql("SELECT * FROM segment_station_link_knn", conn)
    stations = pd.read_sql("SELECT tc_number, aadt FROM historic_stations WHERE year = 2022", conn)
    r1 = predict_idw(knn, stations)
    r2 = predict_idw(knn, stations)
    pd.testing.assert_frame_equal(r1, r2)
    conn.close()


def test_empty_knn_no_crash() -> None:
    """Empty knn input -> empty result, no crash."""
    conn = _setup_db()
    knn = pd.read_sql("SELECT * FROM segment_station_link_knn WHERE 1=0", conn)
    stations = pd.read_sql("SELECT tc_number, aadt FROM historic_stations WHERE year = 2022", conn)
    result = predict_idw(knn, stations)
    assert len(result) == 0
    assert "AADT_MODELED" in result.columns
    conn.close()


def test_segments_row_count_preserved_after_fill() -> None:
    """Writing fill columns back should not change segment row count."""
    conn = _setup_db()
    before = conn.execute("SELECT COUNT(*) FROM segments").fetchone()[0]

    conn.execute("ALTER TABLE segments ADD COLUMN AADT_2022_LOCAL_FILL INTEGER")
    conn.execute("UPDATE segments SET AADT_2022_LOCAL_FILL = NULL")

    knn = pd.read_sql(
        "SELECT * FROM segment_station_link_knn WHERE unique_id IN ('seg_0', 'seg_1')",
        conn,
    )
    stations = pd.read_sql("SELECT tc_number, aadt FROM historic_stations WHERE year = 2022", conn)
    result = predict_idw(knn, stations)

    for _, row in result.iterrows():
        if pd.notna(row["AADT_MODELED"]):
            conn.execute(
                "UPDATE segments SET AADT_2022_LOCAL_FILL = ? WHERE unique_id = ?",
                (int(row["AADT_MODELED"]), row["unique_id"]),
            )
    conn.commit()

    after = conn.execute("SELECT COUNT(*) FROM segments").fetchone()[0]
    assert before == after

    filled = conn.execute("SELECT COUNT(*) FROM segments WHERE AADT_2022_LOCAL_FILL IS NOT NULL").fetchone()[0]
    assert filled == 2
    conn.close()
