"""Checkpoint manifest read/write, fingerprint computation, checkpoint IO."""

from __future__ import annotations

import hashlib
import inspect
import json
import os
import pickle
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import geopandas as gpd

logger = logging.getLogger(__name__)


def hash_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def hash_file_content(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def stat_fingerprint_file(path: Path) -> tuple[int, int]:
    st = os.stat(path)
    return (st.st_size, st.st_mtime_ns)


def stat_fingerprint_directory(path: Path) -> str:
    """Stat-tree fingerprint for a directory (e.g. .gdb).

    Produces a SHA-256 over sorted (relpath, size, mtime_ns) triples for
    every regular file inside, recursed.  Cheap even for multi-GB dirs
    because we never read file contents.
    """
    entries: list[tuple[str, int, int]] = []
    for root, _dirs, files in os.walk(path):
        for fname in files:
            fpath = Path(root) / fname
            if fpath.is_file():
                st = os.stat(fpath)
                rel = fpath.relative_to(path).as_posix()
                entries.append((rel, st.st_size, st.st_mtime_ns))
    entries.sort()
    h = hashlib.sha256()
    for rel, size, mtime_ns in entries:
        h.update(f"{rel}|{size}|{mtime_ns}\n".encode())
    return h.hexdigest()


def fingerprint_raw_input(path: Path) -> dict:
    """Fingerprint a raw input file or directory."""
    if path.is_dir():
        return {
            "path": str(path),
            "kind": "gdb_directory" if path.suffix == ".gdb" else "directory",
            "content_summary_sha256": stat_fingerprint_directory(path),
        }
    else:
        size, mtime_ns = stat_fingerprint_file(path)
        return {
            "path": str(path),
            "kind": "file",
            "size": size,
            "mtime": mtime_ns,
        }


def fingerprint_config_file(path: Path) -> dict:
    return {
        "path": str(path),
        "sha256": hash_file_content(path),
    }


def fingerprint_function(func: Callable) -> dict:
    src = inspect.getsource(func)
    return {
        "module": func.__module__,
        "qualname": func.__qualname__,
        "source_sha256": hash_bytes(src.encode()),
    }


def fingerprint_global(module: Any, name: str) -> dict:
    value = getattr(module, name)
    return {
        "module": module.__name__,
        "name": name,
        "repr": repr(value),
    }


def compute_stage_fingerprint(
    stage_name: str,
    *,
    upstream_fingerprints: list[str],
    raw_inputs: list[dict],
    config_files: list[dict],
    code_functions: list[dict],
    code_globals: list[dict],
    code_files: list[dict],
    stage_params: dict | None = None,
) -> str:
    """Compute a deterministic SHA-256 fingerprint for a stage."""
    h = hashlib.sha256()
    h.update(stage_name.encode())

    for fp in upstream_fingerprints:
        h.update(fp.encode())

    for entry in sorted(raw_inputs, key=lambda e: e["path"]):
        h.update(json.dumps(entry, sort_keys=True).encode())

    for entry in sorted(config_files, key=lambda e: e["path"]):
        h.update(json.dumps(entry, sort_keys=True).encode())

    for entry in sorted(code_functions, key=lambda e: f"{e['module']}.{e['qualname']}"):
        h.update(json.dumps(entry, sort_keys=True).encode())

    for entry in sorted(code_globals, key=lambda e: f"{e['module']}.{e['name']}"):
        h.update(json.dumps(entry, sort_keys=True).encode())

    for entry in sorted(code_files, key=lambda e: e["path"]):
        h.update(json.dumps(entry, sort_keys=True).encode())

    if stage_params:
        h.update(json.dumps(stage_params, sort_keys=True).encode())

    return f"sha256:{h.hexdigest()}"


def write_manifest(
    manifest_path: Path,
    *,
    stage_name: str,
    fingerprint: str,
    runtime_seconds: float,
    inputs: dict,
    output: dict,
) -> None:
    manifest = {
        "stage_name": stage_name,
        "fingerprint": fingerprint,
        "produced_at": datetime.now(timezone.utc).isoformat(),
        "runtime_seconds": round(runtime_seconds, 2),
        "inputs": inputs,
        "output": output,
    }
    tmp_path = manifest_path.with_suffix(".json.tmp")
    tmp_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    tmp_path.replace(manifest_path)


def read_manifest(manifest_path: Path) -> dict | None:
    if not manifest_path.exists():
        return None
    try:
        return json.loads(manifest_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


def write_checkpoint(path: Path, gdf: gpd.GeoDataFrame) -> dict:
    """Write a GeoDataFrame checkpoint as GeoParquet. Returns output metadata."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(".parquet.tmp")
    gdf.to_parquet(tmp_path, engine="pyarrow")
    tmp_path.replace(path)
    return {
        "path": str(path),
        "row_count": len(gdf),
        "column_count": len(gdf.columns),
        "crs": str(gdf.crs) if gdf.crs else None,
        "sha256": hash_file_content(path),
    }


def read_checkpoint(path: Path) -> gpd.GeoDataFrame:
    return gpd.read_parquet(path)


def cleanup_stale_temps(checkpoint_dir: Path) -> None:
    """Remove stale .tmp files left by interrupted writes."""
    if not checkpoint_dir.exists():
        return
    for tmp_file in checkpoint_dir.rglob("*.tmp"):
        logger.warning("Removing stale temp file: %s", tmp_file)
        tmp_file.unlink(missing_ok=True)
