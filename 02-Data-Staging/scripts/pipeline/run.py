"""CLI entrypoint for the checkpoint-aware pipeline runner.

Usage:
    python 02-Data-Staging/scripts/pipeline/run.py --pipeline roadway_inventory
    python 02-Data-Staging/scripts/pipeline/run.py --pipeline roadway_inventory --force-all
    python 02-Data-Staging/scripts/pipeline/run.py --pipeline roadway_inventory --output-root /tmp/staging
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]

sys.path.insert(0, str(REPO_ROOT / "02-Data-Staging" / "scripts" / "01_roadway_inventory"))

logger = logging.getLogger(__name__)


def _redirect_output_dirs(output_root: Path) -> None:
    """Override normalize.py module-level dir constants for output redirection."""
    import normalize

    staging_root = output_root / "02-Data-Staging"
    tmp_root = output_root / ".tmp"

    normalize.TABLES_DIR = staging_root / "tables"
    normalize.SPATIAL_DIR = staging_root / "spatial"
    normalize.REPORTS_DIR = staging_root / "reports"
    normalize.CONFIG_DIR = staging_root / "config"
    normalize.REBUILD_OUTPUTS_DIR = tmp_root / "rebuild_outputs"
    normalize.CURRENT_AADT_AUDIT_DIR = tmp_root / "roadway_inventory" / "current_aadt_audit"


def _detect_worktree_without_redirect() -> bool:
    """Warn if running from a worktree without --output-root."""
    git_dir = REPO_ROOT / ".git"
    if git_dir.is_file():
        return True
    return False


def run_pipeline(pipeline_name: str, *, force_all: bool, output_root: Path | None) -> None:
    if pipeline_name != "roadway_inventory":
        logger.error("Unknown pipeline: %s (only 'roadway_inventory' is supported in M1)", pipeline_name)
        sys.exit(1)

    if output_root:
        _redirect_output_dirs(output_root)
        logger.info("Output redirected to: %s", output_root)
    elif _detect_worktree_without_redirect():
        logger.warning(
            "Running from a git worktree without --output-root. "
            "Outputs will write to the MAIN repo's staging dirs. "
            "Use --output-root to redirect if this is not intended."
        )

    from .checkpoint import cleanup_stale_temps
    from .stage import run_stage
    from .stages.roadway_inventory import registry

    checkpoint_dir = REPO_ROOT / "02-Data-Staging" / "staged" / "checkpoints" / "01_roadway_inventory"
    if output_root:
        checkpoint_dir = output_root / "02-Data-Staging" / "staged" / "checkpoints" / "01_roadway_inventory"

    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    cleanup_stale_temps(checkpoint_dir)

    context: dict = {}
    results: dict = {}

    t_total = time.perf_counter()

    for stage_def in registry.stages:
        result = run_stage(
            stage_def,
            checkpoint_dir=checkpoint_dir,
            upstream_results=results,
            force=force_all,
            context=context,
        )
        results[stage_def.name] = result

    elapsed = time.perf_counter() - t_total
    hit_count = sum(1 for r in results.values() if r.skipped)
    miss_count = sum(1 for r in results.values() if not r.skipped)
    logger.info(
        "Pipeline complete in %.1fs (%d stages: %d cache hits, %d executed)",
        elapsed, len(results), hit_count, miss_count,
    )


def main():
    parser = argparse.ArgumentParser(
        description="Checkpoint-aware pipeline runner (M1)",
    )
    parser.add_argument(
        "--pipeline",
        required=True,
        help="Pipeline to run (e.g. roadway_inventory)",
    )
    parser.add_argument(
        "--force-all",
        action="store_true",
        help="Ignore all manifests and rerun every stage",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=None,
        help="Redirect all output writes to this root (for worktree runs)",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s: %(message)s",
    )

    run_pipeline(
        args.pipeline,
        force_all=args.force_all,
        output_root=args.output_root,
    )


if __name__ == "__main__":
    main()
