"""@stage decorator: fingerprint check, skip-on-hit, manifest write."""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any, Callable

import geopandas as gpd

from pipeline import checkpoint as ckpt

logger = logging.getLogger(__name__)


class StageResult:
    """Container returned by a stage execution."""

    __slots__ = ("name", "fingerprint", "data", "skipped")

    def __init__(self, name: str, fingerprint: str, data: Any, skipped: bool):
        self.name = name
        self.fingerprint = fingerprint
        self.data = data
        self.skipped = skipped


class StageRegistry:
    """Ordered registry of stages for a pipeline."""

    def __init__(self):
        self._stages: list[StageDefinition] = []
        self._by_name: dict[str, StageDefinition] = {}

    def add(self, stage_def: "StageDefinition"):
        self._stages.append(stage_def)
        self._by_name[stage_def.name] = stage_def

    def get(self, name: str) -> "StageDefinition":
        return self._by_name[name]

    @property
    def stages(self) -> list["StageDefinition"]:
        return list(self._stages)


class StageDefinition:
    """Declarative stage metadata and execution logic."""

    def __init__(
        self,
        name: str,
        func: Callable,
        *,
        upstream: list[str] | None = None,
        raw_inputs: list[Path] | None = None,
        config_files: list[Path] | None = None,
        helpers: list[Callable] | None = None,
        globals_list: list[tuple[Any, str]] | None = None,
        code_files: list[Path] | None = None,
        produces_geodataframe: bool = True,
        context_save: Callable | None = None,
        context_restore: Callable | None = None,
    ):
        self.name = name
        self.func = func
        self.upstream = upstream or []
        self.raw_inputs = raw_inputs or []
        self.config_files = config_files or []
        self.helpers = helpers or []
        self.globals_list = globals_list or []
        self.code_files = code_files or []
        self.produces_geodataframe = produces_geodataframe
        self.context_save = context_save
        self.context_restore = context_restore


def _resolve_checkpoint_path(checkpoint_dir: Path, stage_name: str) -> Path:
    return checkpoint_dir / f"{stage_name}.parquet"


def _resolve_manifest_path(checkpoint_dir: Path, stage_name: str) -> Path:
    return checkpoint_dir / f"{stage_name}.manifest.json"


def run_stage(
    stage_def: StageDefinition,
    *,
    checkpoint_dir: Path,
    upstream_results: dict[str, StageResult],
    force: bool = False,
    context: dict[str, Any] | None = None,
) -> StageResult:
    """Execute a stage with fingerprint-based caching.

    Args:
        stage_def: The stage definition.
        checkpoint_dir: Where to read/write checkpoints.
        upstream_results: Results from already-executed upstream stages.
        force: If True, skip cache lookup.
        context: Runtime context dict passed to the stage function.

    Returns:
        StageResult with the stage output.
    """
    context = context if context is not None else {}

    upstream_fingerprints = [
        upstream_results[up].fingerprint for up in stage_def.upstream
    ]

    raw_input_entries = []
    for path in stage_def.raw_inputs:
        if path.exists():
            raw_input_entries.append(ckpt.fingerprint_raw_input(path))

    config_entries = []
    for path in stage_def.config_files:
        if path.exists():
            config_entries.append(ckpt.fingerprint_config_file(path))

    func_entries = [ckpt.fingerprint_function(f) for f in stage_def.helpers]

    global_entries = [
        ckpt.fingerprint_global(mod, name)
        for mod, name in stage_def.globals_list
    ]

    code_file_entries = []
    for path in stage_def.code_files:
        if path.exists():
            code_file_entries.append({
                "path": str(path),
                "sha256": ckpt.hash_file_content(path),
            })

    fingerprint = ckpt.compute_stage_fingerprint(
        stage_def.name,
        upstream_fingerprints=upstream_fingerprints,
        raw_inputs=raw_input_entries,
        config_files=config_entries,
        code_functions=func_entries,
        code_globals=global_entries,
        code_files=code_file_entries,
    )

    checkpoint_path = _resolve_checkpoint_path(checkpoint_dir, stage_def.name)
    manifest_path = _resolve_manifest_path(checkpoint_dir, stage_def.name)

    if not force:
        existing_manifest = ckpt.read_manifest(manifest_path)
        if (
            existing_manifest is not None
            and existing_manifest.get("fingerprint") == fingerprint
            and (not stage_def.produces_geodataframe or checkpoint_path.exists())
        ):
            age_str = ""
            produced_at = existing_manifest.get("produced_at", "")
            if produced_at:
                try:
                    from datetime import datetime, timezone
                    produced = datetime.fromisoformat(produced_at)
                    age = datetime.now(timezone.utc) - produced
                    hours = age.total_seconds() / 3600
                    if hours < 1:
                        age_str = f" (manifest age {int(age.total_seconds() / 60)}m)"
                    else:
                        age_str = f" (manifest age {hours:.1f}h)"
                except (ValueError, TypeError):
                    pass

            logger.info(
                "[stage %s] fingerprint %s → CACHE HIT, skipping%s",
                stage_def.name, fingerprint[:20], age_str,
            )

            if stage_def.produces_geodataframe:
                data = ckpt.read_checkpoint(checkpoint_path)
            else:
                data = None
            if stage_def.context_restore:
                stage_def.context_restore(checkpoint_dir, context)
            return StageResult(stage_def.name, fingerprint, data, skipped=True)

    logger.info(
        "[stage %s] fingerprint %s → MISS, running",
        stage_def.name, fingerprint[:20],
    )

    t0 = time.perf_counter()
    data = stage_def.func(upstream_results=upstream_results, context=context)
    elapsed = time.perf_counter() - t0

    inputs_record = {
        "upstream_checkpoints": [
            {"stage": up, "fingerprint": upstream_results[up].fingerprint}
            for up in stage_def.upstream
        ],
        "raw_files": raw_input_entries,
        "config_files": config_entries,
        "code_functions": func_entries,
        "code_globals": global_entries,
        "code_files": code_file_entries,
    }

    if stage_def.produces_geodataframe and isinstance(data, gpd.GeoDataFrame):
        output_meta = ckpt.write_checkpoint(checkpoint_path, data)
    else:
        output_meta = {"path": None, "note": "no geodataframe output"}

    if stage_def.context_save:
        stage_def.context_save(checkpoint_dir, context)

    ckpt.write_manifest(
        manifest_path,
        stage_name=stage_def.name,
        fingerprint=fingerprint,
        runtime_seconds=elapsed,
        inputs=inputs_record,
        output=output_meta,
    )

    logger.info(
        "[stage %s] complete in %.1fs, wrote %s",
        stage_def.name, elapsed,
        checkpoint_path if stage_def.produces_geodataframe else "manifest only",
    )

    return StageResult(stage_def.name, fingerprint, data, skipped=False)
