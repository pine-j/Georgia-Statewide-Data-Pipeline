"""Tests for pipeline.stage module."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import Point

from pipeline.checkpoint import write_checkpoint, write_manifest, compute_stage_fingerprint
from pipeline.stage import StageDefinition, StageRegistry, StageResult, run_stage


# ── helpers ──────────────────────────────────────────────────────────────────

def _make_gdf(n: int = 3) -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {"val": list(range(n))},
        geometry=[Point(i, i) for i in range(n)],
        crs="EPSG:4326",
    )


def _noop_stage(*, upstream_results, context):
    """Minimal stage function that returns a GeoDataFrame."""
    return _make_gdf(4)


def _noop_non_geo_stage(*, upstream_results, context):
    """Stage that doesn't produce a GeoDataFrame."""
    context["side_effect"] = True
    return None


def _simple_stage_def(name: str = "test_stage", func=None, **kwargs) -> StageDefinition:
    return StageDefinition(name=name, func=func or _noop_stage, **kwargs)


# ── 1. StageRegistry ordering ───────────────────────────────────────────────

def test_registry_insertion_order():
    reg = StageRegistry()
    s1 = _simple_stage_def("a")
    s2 = _simple_stage_def("b")
    s3 = _simple_stage_def("c")
    reg.add(s1)
    reg.add(s2)
    reg.add(s3)
    assert [s.name for s in reg.stages] == ["a", "b", "c"]


def test_registry_get():
    reg = StageRegistry()
    s1 = _simple_stage_def("alpha")
    reg.add(s1)
    assert reg.get("alpha") is s1


# ── 2. run_stage cold miss ──────────────────────────────────────────────────

def test_run_stage_cold_miss(tmp_path: Path):
    sd = _simple_stage_def("cold_test")
    result = run_stage(
        sd,
        checkpoint_dir=tmp_path,
        upstream_results={},
    )
    assert result.skipped is False
    assert result.name == "cold_test"
    assert isinstance(result.data, gpd.GeoDataFrame)
    assert len(result.data) == 4
    # Manifest and checkpoint written
    assert (tmp_path / "cold_test.manifest.json").exists()
    assert (tmp_path / "cold_test.parquet").exists()


# ── 3. run_stage cache hit ──────────────────────────────────────────────────

def test_run_stage_cache_hit(tmp_path: Path):
    sd = _simple_stage_def("cached")

    # First run — cold miss
    r1 = run_stage(sd, checkpoint_dir=tmp_path, upstream_results={})
    assert r1.skipped is False

    # Second run — should hit cache
    call_count = 0
    original_func = sd.func

    def counting_func(*, upstream_results, context):
        nonlocal call_count
        call_count += 1
        return original_func(upstream_results=upstream_results, context=context)

    sd.func = counting_func
    r2 = run_stage(sd, checkpoint_dir=tmp_path, upstream_results={})
    assert r2.skipped is True
    assert call_count == 0  # func was NOT called
    assert isinstance(r2.data, gpd.GeoDataFrame)
    assert r2.fingerprint == r1.fingerprint


# ── 4. run_stage fingerprint mismatch ───────────────────────────────────────

def test_run_stage_fingerprint_mismatch(tmp_path: Path):
    sd = _simple_stage_def("mismatch")

    # First run
    r1 = run_stage(sd, checkpoint_dir=tmp_path, upstream_results={})
    assert r1.skipped is False

    # Change the function to alter fingerprint
    def altered_func(*, upstream_results, context):
        return _make_gdf(7)

    sd.func = altered_func
    # Need to also change helpers to actually change fingerprint
    # Since func is not in helpers, we change helpers list
    sd.helpers = [altered_func]

    r2 = run_stage(sd, checkpoint_dir=tmp_path, upstream_results={})
    assert r2.skipped is False
    assert r2.fingerprint != r1.fingerprint


# ── 5. run_stage force=True ─────────────────────────────────────────────────

def test_run_stage_force_bypasses_cache(tmp_path: Path):
    sd = _simple_stage_def("force_test")

    # First run
    run_stage(sd, checkpoint_dir=tmp_path, upstream_results={})

    # Force re-run — func must be called
    call_count = 0

    def counting_func(*, upstream_results, context):
        nonlocal call_count
        call_count += 1
        return _make_gdf(4)

    sd.func = counting_func
    r2 = run_stage(sd, checkpoint_dir=tmp_path, upstream_results={}, force=True)
    assert r2.skipped is False
    assert call_count == 1


# ── 6. run_stage non-geodataframe ───────────────────────────────────────────

def test_run_stage_non_geodataframe(tmp_path: Path):
    sd = _simple_stage_def(
        "no_geo",
        func=_noop_non_geo_stage,
        produces_geodataframe=False,
    )
    ctx: dict[str, Any] = {}
    result = run_stage(
        sd,
        checkpoint_dir=tmp_path,
        upstream_results={},
        context=ctx,
    )
    assert result.skipped is False
    # No parquet written
    assert not (tmp_path / "no_geo.parquet").exists()
    # Manifest IS written
    assert (tmp_path / "no_geo.manifest.json").exists()
    # Side effect visible
    assert ctx.get("side_effect") is True


def test_run_stage_non_geodataframe_cache_hit(tmp_path: Path):
    sd = _simple_stage_def(
        "no_geo_hit",
        func=_noop_non_geo_stage,
        produces_geodataframe=False,
    )

    # First run
    run_stage(sd, checkpoint_dir=tmp_path, upstream_results={}, context={})

    # Second run — cache hit even without parquet
    call_count = 0
    original = sd.func

    def counting(*, upstream_results, context):
        nonlocal call_count
        call_count += 1
        return original(upstream_results=upstream_results, context=context)

    sd.func = counting
    r2 = run_stage(sd, checkpoint_dir=tmp_path, upstream_results={}, context={})
    assert r2.skipped is True
    assert call_count == 0
    assert r2.data is None


# ── 7. upstream propagation ─────────────────────────────────────────────────

def test_upstream_fingerprint_change_invalidates_downstream(tmp_path: Path):
    # Simulate upstream result with fingerprint A
    up_a = StageResult("upstream", "sha256:aaa", None, skipped=True)

    sd = _simple_stage_def("downstream", upstream=["upstream"])
    r1 = run_stage(
        sd,
        checkpoint_dir=tmp_path,
        upstream_results={"upstream": up_a},
    )

    # Change upstream fingerprint
    up_b = StageResult("upstream", "sha256:bbb", None, skipped=True)
    r2 = run_stage(
        sd,
        checkpoint_dir=tmp_path,
        upstream_results={"upstream": up_b},
    )
    # Fingerprints differ → downstream must re-run
    assert r1.fingerprint != r2.fingerprint
    assert r2.skipped is False


# ── 8. Context dict passing ─────────────────────────────────────────────────

def test_context_dict_passed_and_mutable(tmp_path: Path):
    def mutating_stage(*, upstream_results, context):
        context["ran"] = True
        return _make_gdf(1)

    sd = _simple_stage_def("ctx_test", func=mutating_stage)
    ctx: dict[str, Any] = {"initial": 1}
    run_stage(sd, checkpoint_dir=tmp_path, upstream_results={}, context=ctx)
    assert ctx["ran"] is True
    assert ctx["initial"] == 1
