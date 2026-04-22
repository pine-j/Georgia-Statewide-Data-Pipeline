"""Tests for pipeline.checkpoint module."""

from __future__ import annotations

import os
import time
import types
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import Point

# ── Imports ──────────────────────────────────────────────────────────────────
from pipeline.checkpoint import (
    cleanup_stale_temps,
    compute_stage_fingerprint,
    fingerprint_config_file,
    fingerprint_function,
    fingerprint_global,
    fingerprint_raw_input,
    hash_bytes,
    read_checkpoint,
    read_manifest,
    stat_fingerprint_directory,
    write_checkpoint,
    write_manifest,
)


# ── helpers ──────────────────────────────────────────────────────────────────

def _make_gdf(n: int = 3) -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {"val": list(range(n))},
        geometry=[Point(i, i) for i in range(n)],
        crs="EPSG:4326",
    )


# ── 1. hash_bytes determinism ───────────────────────────────────────────────

def test_hash_bytes_deterministic():
    assert hash_bytes(b"hello") == hash_bytes(b"hello")


def test_hash_bytes_different_input():
    assert hash_bytes(b"hello") != hash_bytes(b"world")


# ── 2. stat_fingerprint_directory ────────────────────────────────────────────

def test_stat_fingerprint_directory_stable(tmp_path: Path):
    d = tmp_path / "mydir"
    d.mkdir()
    (d / "a.txt").write_text("aaa")
    (d / "b.txt").write_text("bbb")
    fp1 = stat_fingerprint_directory(d)
    fp2 = stat_fingerprint_directory(d)
    assert fp1 == fp2


def test_stat_fingerprint_directory_changes_on_touch(tmp_path: Path):
    d = tmp_path / "mydir"
    d.mkdir()
    f = d / "a.txt"
    f.write_text("aaa")
    fp1 = stat_fingerprint_directory(d)

    # Ensure mtime actually changes (Windows timer resolution can be coarse)
    time.sleep(0.05)
    f.write_text("aaa_modified")
    fp2 = stat_fingerprint_directory(d)
    assert fp1 != fp2


# ── 3. fingerprint_raw_input file vs directory ──────────────────────────────

def test_fingerprint_raw_input_file(tmp_path: Path):
    f = tmp_path / "data.csv"
    f.write_text("col1,col2\n1,2\n")
    result = fingerprint_raw_input(f)
    assert result["kind"] == "file"
    assert "size" in result
    assert "mtime" in result


def test_fingerprint_raw_input_directory(tmp_path: Path):
    d = tmp_path / "test.gdb"
    d.mkdir()
    (d / "table.gdbtable").write_bytes(b"\x00\x01\x02")
    result = fingerprint_raw_input(d)
    assert result["kind"] == "gdb_directory"
    assert "content_summary_sha256" in result


def test_fingerprint_raw_input_plain_directory(tmp_path: Path):
    d = tmp_path / "stuff"
    d.mkdir()
    (d / "f.txt").write_text("hi")
    result = fingerprint_raw_input(d)
    assert result["kind"] == "directory"


# ── 4. fingerprint_config_file ──────────────────────────────────────────────

def test_fingerprint_config_file(tmp_path: Path):
    f = tmp_path / "config.json"
    f.write_text('{"key": "value"}')
    result = fingerprint_config_file(f)
    assert "sha256" in result
    # Same content → same hash
    f2 = tmp_path / "config2.json"
    f2.write_text('{"key": "value"}')
    assert fingerprint_config_file(f2)["sha256"] == result["sha256"]


def test_fingerprint_config_file_changes(tmp_path: Path):
    f = tmp_path / "config.json"
    f.write_text('{"key": "value"}')
    h1 = fingerprint_config_file(f)["sha256"]
    f.write_text('{"key": "other"}')
    h2 = fingerprint_config_file(f)["sha256"]
    assert h1 != h2


# ── 5. fingerprint_function ─────────────────────────────────────────────────

def _sample_func_a():
    return 1


def _sample_func_b():
    return 2


def test_fingerprint_function_deterministic():
    fp1 = fingerprint_function(_sample_func_a)
    fp2 = fingerprint_function(_sample_func_a)
    assert fp1 == fp2


def test_fingerprint_function_different_body():
    fp_a = fingerprint_function(_sample_func_a)
    fp_b = fingerprint_function(_sample_func_b)
    assert fp_a["source_sha256"] != fp_b["source_sha256"]


# ── 6. fingerprint_global ───────────────────────────────────────────────────

def test_fingerprint_global_captures_repr():
    mod = types.ModuleType("fake_mod")
    mod.MY_CONST = 42
    result = fingerprint_global(mod, "MY_CONST")
    assert result["repr"] == "42"
    assert result["name"] == "MY_CONST"


def test_fingerprint_global_changes_with_value():
    mod = types.ModuleType("fake_mod")
    mod.X = [1, 2]
    r1 = fingerprint_global(mod, "X")
    mod.X = [1, 2, 3]
    r2 = fingerprint_global(mod, "X")
    assert r1["repr"] != r2["repr"]


# ── 7. compute_stage_fingerprint determinism ────────────────────────────────

_BASE_FP_KWARGS = dict(
    upstream_fingerprints=["sha256:aaa"],
    raw_inputs=[{"path": "/a", "kind": "file", "size": 10, "mtime": 100}],
    config_files=[{"path": "/c.json", "sha256": "deadbeef"}],
    code_functions=[{"module": "m", "qualname": "f", "source_sha256": "abc"}],
    code_globals=[{"module": "m", "name": "X", "repr": "42"}],
    code_files=[{"path": "/m.py", "sha256": "fff"}],
)


def test_compute_stage_fingerprint_deterministic():
    fp1 = compute_stage_fingerprint("stage1", **_BASE_FP_KWARGS)
    fp2 = compute_stage_fingerprint("stage1", **_BASE_FP_KWARGS)
    assert fp1 == fp2
    assert fp1.startswith("sha256:")


# ── 8. compute_stage_fingerprint sensitivity ────────────────────────────────

def test_fingerprint_sensitive_to_stage_name():
    fp1 = compute_stage_fingerprint("stage1", **_BASE_FP_KWARGS)
    fp2 = compute_stage_fingerprint("stage2", **_BASE_FP_KWARGS)
    assert fp1 != fp2


def test_fingerprint_sensitive_to_upstream():
    kwargs = {**_BASE_FP_KWARGS, "upstream_fingerprints": ["sha256:bbb"]}
    fp1 = compute_stage_fingerprint("stage1", **_BASE_FP_KWARGS)
    fp2 = compute_stage_fingerprint("stage1", **kwargs)
    assert fp1 != fp2


def test_fingerprint_sensitive_to_raw_input():
    alt = [{"path": "/b", "kind": "file", "size": 20, "mtime": 200}]
    fp1 = compute_stage_fingerprint("stage1", **_BASE_FP_KWARGS)
    fp2 = compute_stage_fingerprint("stage1", **{**_BASE_FP_KWARGS, "raw_inputs": alt})
    assert fp1 != fp2


def test_fingerprint_sensitive_to_config():
    alt = [{"path": "/c.json", "sha256": "changed"}]
    fp1 = compute_stage_fingerprint("stage1", **_BASE_FP_KWARGS)
    fp2 = compute_stage_fingerprint("stage1", **{**_BASE_FP_KWARGS, "config_files": alt})
    assert fp1 != fp2


def test_fingerprint_sensitive_to_code_function():
    alt = [{"module": "m", "qualname": "f", "source_sha256": "xyz"}]
    fp1 = compute_stage_fingerprint("stage1", **_BASE_FP_KWARGS)
    fp2 = compute_stage_fingerprint("stage1", **{**_BASE_FP_KWARGS, "code_functions": alt})
    assert fp1 != fp2


def test_fingerprint_sensitive_to_globals():
    alt = [{"module": "m", "name": "X", "repr": "99"}]
    fp1 = compute_stage_fingerprint("stage1", **_BASE_FP_KWARGS)
    fp2 = compute_stage_fingerprint("stage1", **{**_BASE_FP_KWARGS, "code_globals": alt})
    assert fp1 != fp2


# ── 9. write_checkpoint / read_checkpoint round-trip ────────────────────────

def test_checkpoint_roundtrip(tmp_path: Path):
    gdf = _make_gdf(5)
    cp = tmp_path / "stage.parquet"
    meta = write_checkpoint(cp, gdf)

    assert meta["row_count"] == 5
    assert meta["column_count"] == 2  # val + geometry
    assert meta["crs"] == "EPSG:4326"
    assert cp.exists()

    loaded = read_checkpoint(cp)
    pd.testing.assert_frame_equal(gdf.drop(columns="geometry"), loaded.drop(columns="geometry"))
    assert loaded.crs.to_epsg() == 4326


# ── 10. write_manifest / read_manifest round-trip ───────────────────────────

def test_manifest_roundtrip(tmp_path: Path):
    mp = tmp_path / "stage.manifest.json"
    write_manifest(
        mp,
        stage_name="test_stage",
        fingerprint="sha256:abc123",
        runtime_seconds=1.234,
        inputs={"upstream_checkpoints": []},
        output={"row_count": 10},
    )

    m = read_manifest(mp)
    assert m is not None
    assert m["stage_name"] == "test_stage"
    assert m["fingerprint"] == "sha256:abc123"
    assert m["runtime_seconds"] == 1.23  # rounded to 2dp
    assert "produced_at" in m


def test_read_manifest_missing(tmp_path: Path):
    assert read_manifest(tmp_path / "nope.json") is None


def test_read_manifest_corrupt(tmp_path: Path):
    f = tmp_path / "bad.json"
    f.write_text("NOT JSON{{{")
    assert read_manifest(f) is None


# ── 11. cleanup_stale_temps ─────────────────────────────────────────────────

def test_cleanup_stale_temps(tmp_path: Path):
    (tmp_path / "a.parquet.tmp").write_bytes(b"junk")
    (tmp_path / "b.json.tmp").write_bytes(b"junk")
    (tmp_path / "good.parquet").write_bytes(b"real")

    cleanup_stale_temps(tmp_path)

    assert not (tmp_path / "a.parquet.tmp").exists()
    assert not (tmp_path / "b.json.tmp").exists()
    assert (tmp_path / "good.parquet").exists()


def test_cleanup_stale_temps_nonexistent_dir(tmp_path: Path):
    # Should not raise
    cleanup_stale_temps(tmp_path / "does_not_exist")


# ── 12. Atomic write safety ────────────────────────────────────────────────

def test_checkpoint_atomic_write_no_leftover_tmp(tmp_path: Path):
    gdf = _make_gdf(2)
    cp = tmp_path / "stage.parquet"
    write_checkpoint(cp, gdf)
    # .tmp should be renamed away
    assert not (tmp_path / "stage.parquet.tmp").exists()
    assert cp.exists()


def test_manifest_atomic_write_no_leftover_tmp(tmp_path: Path):
    mp = tmp_path / "stage.manifest.json"
    write_manifest(
        mp,
        stage_name="s",
        fingerprint="sha256:x",
        runtime_seconds=0.1,
        inputs={},
        output={},
    )
    assert not (tmp_path / "stage.manifest.json.tmp").exists()
    assert mp.exists()
