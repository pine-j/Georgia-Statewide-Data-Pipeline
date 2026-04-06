"""Build AOI review packages from base_network.gpkg for roadway gap analysis.

This utility does not try to infer missing roads from a basemap. It clips the
current staged roadway network to one or more AOIs and writes review artifacts
so a person can decide whether apparent gaps are mostly minor local streets or
planning-relevant missing links.

Generated outputs default to:
    .tmp/roadway_gap_fill/aoi_roadway_reviews/

That keeps exploratory review artifacts out of tracked project folders.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import geopandas as gpd
import pandas as pd
from shapely.geometry import box

LOG = logging.getLogger("aoi_roadway_coverage")

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_GPKG = PROJECT_ROOT / "02-Data-Staging" / "spatial" / "base_network.gpkg"
DEFAULT_LAYER = "roadway_segments"
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / ".tmp" / "roadway_gap_fill" / "aoi_roadway_reviews"
TARGET_CRS = "EPSG:32617"


@dataclass(frozen=True)
class AoiSpec:
    aoi_id: str
    xmin: float
    ymin: float
    xmax: float
    ymax: float
    srid: int
    notes: str = ""


def configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        force=True,
    )


def read_aoi_csv(path: Path) -> list[AoiSpec]:
    rows: list[AoiSpec] = []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        required = {"aoi_id", "xmin", "ymin", "xmax", "ymax"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"AOI CSV missing columns: {sorted(missing)}")

        for record in reader:
            rows.append(
                AoiSpec(
                    aoi_id=str(record["aoi_id"]).strip(),
                    xmin=float(record["xmin"]),
                    ymin=float(record["ymin"]),
                    xmax=float(record["xmax"]),
                    ymax=float(record["ymax"]),
                    srid=int(record.get("srid") or 4326),
                    notes=str(record.get("notes") or "").strip(),
                )
            )
    return rows


def parse_single_aoi(args: argparse.Namespace) -> list[AoiSpec]:
    if args.bbox is None:
        raise ValueError("Provide either --aoi-csv or --bbox.")
    xmin, ymin, xmax, ymax = args.bbox
    return [
        AoiSpec(
            aoi_id=args.aoi_id or "aoi_1",
            xmin=float(xmin),
            ymin=float(ymin),
            xmax=float(xmax),
            ymax=float(ymax),
            srid=int(args.bbox_srid),
            notes=args.notes or "",
        )
    ]


def load_aois(args: argparse.Namespace) -> list[AoiSpec]:
    if args.aoi_csv:
        return read_aoi_csv(Path(args.aoi_csv))
    return parse_single_aoi(args)


def load_network(gpkg_path: Path, layer: str) -> gpd.GeoDataFrame:
    if not gpkg_path.exists():
        raise FileNotFoundError(f"GeoPackage not found: {gpkg_path}")
    gdf = gpd.read_file(gpkg_path, layer=layer, engine="pyogrio")
    if gdf.crs is None:
        raise ValueError(f"{gpkg_path}:{layer} has no CRS.")
    return gdf


def aoi_polygon(spec: AoiSpec) -> gpd.GeoDataFrame:
    geom = gpd.GeoDataFrame(
        [{"aoi_id": spec.aoi_id, "notes": spec.notes}],
        geometry=[box(spec.xmin, spec.ymin, spec.xmax, spec.ymax)],
        crs=f"EPSG:{spec.srid}",
    )
    return geom.to_crs(TARGET_CRS)


def prepare_length_column(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    output = gdf.copy()
    # Recompute from clipped geometry so AOI summaries reflect in-AOI length.
    output["segment_length_mi_calc"] = output.geometry.length / 1609.344
    return output


def summarize_group(gdf: gpd.GeoDataFrame, column: str) -> list[dict[str, Any]]:
    if column not in gdf.columns:
        return []

    grouped = (
        gdf.assign(_group=gdf[column].fillna("NULL").astype(str))
        .groupby("_group", dropna=False)
        .agg(
            feature_count=("geometry", "size"),
            total_centerline_miles=("segment_length_mi_calc", "sum"),
        )
        .reset_index()
        .rename(columns={"_group": column})
        .sort_values(["total_centerline_miles", "feature_count"], ascending=[False, False])
    )
    grouped["total_centerline_miles"] = grouped["total_centerline_miles"].round(3)
    return grouped.to_dict(orient="records")


def top_named_segments(gdf: gpd.GeoDataFrame, limit: int = 15) -> list[dict[str, Any]]:
    name_col = "ROUTE_ID" if "ROUTE_ID" in gdf.columns else None
    if "Comments" in gdf.columns:
        display_name = gdf["Comments"].fillna("")
    elif name_col:
        display_name = gdf[name_col].fillna("")
    else:
        display_name = pd.Series([""] * len(gdf), index=gdf.index)

    sample = (
        gdf.assign(
            display_name=display_name,
            route_id=gdf["ROUTE_ID"].fillna("").astype(str) if "ROUTE_ID" in gdf.columns else "",
            functional_class=gdf["FUNCTIONAL_CLASS"].fillna("").astype(str)
            if "FUNCTIONAL_CLASS" in gdf.columns
            else "",
            system_code=gdf["SYSTEM_CODE"].fillna("").astype(str) if "SYSTEM_CODE" in gdf.columns else "",
        )
        .sort_values("segment_length_mi_calc", ascending=False)
        .head(limit)
    )

    rows: list[dict[str, Any]] = []
    for _, record in sample.iterrows():
        rows.append(
            {
                "display_name": record.get("display_name", ""),
                "route_id": record.get("route_id", ""),
                "functional_class": record.get("functional_class", ""),
                "system_code": record.get("system_code", ""),
                "segment_length_mi": round(float(record["segment_length_mi_calc"]), 3),
            }
        )
    return rows


def write_review_template(
    output_path: Path,
    spec: AoiSpec,
    summary: dict[str, Any],
) -> None:
    rows = [
        {
            "aoi_id": spec.aoi_id,
            "review_status": "pending",
            "reviewer": "",
            "review_date": "",
            "bbox_input_srid": spec.srid,
            "bbox_input": f"{spec.xmin},{spec.ymin},{spec.xmax},{spec.ymax}",
            "current_feature_count": summary["feature_count"],
            "current_centerline_miles": summary["total_centerline_miles"],
            "visible_gap_observed": "",
            "gap_description": "",
            "missing_road_pattern": "",
            "planning_relevance": "",
            "planning_relevance_reason": "",
            "likely_missing_types": "",
            "major_generators_or_facilities_affected": "",
            "recommended_action": "",
            "notes": spec.notes,
        }
    ]
    pd.DataFrame(rows).to_csv(output_path, index=False)


def build_summary(spec: AoiSpec, clipped: gpd.GeoDataFrame) -> dict[str, Any]:
    clipped = prepare_length_column(clipped)

    feature_count = int(len(clipped))
    total_miles = round(float(clipped["segment_length_mi_calc"].sum()), 3) if feature_count else 0.0
    named_route_count = int(clipped["ROUTE_ID"].notna().sum()) if "ROUTE_ID" in clipped.columns else 0
    aadt_non_null = (
        int(pd.to_numeric(clipped["AADT"], errors="coerce").notna().sum()) if "AADT" in clipped.columns else 0
    )

    return {
        "aoi_id": spec.aoi_id,
        "notes": spec.notes,
        "input_bbox": {
            "xmin": spec.xmin,
            "ymin": spec.ymin,
            "xmax": spec.xmax,
            "ymax": spec.ymax,
            "srid": spec.srid,
        },
        "feature_count": feature_count,
        "total_centerline_miles": total_miles,
        "named_route_count": named_route_count,
        "aadt_non_null_count": aadt_non_null,
        "functional_class_summary": summarize_group(clipped, "FUNCTIONAL_CLASS"),
        "system_code_summary": summarize_group(clipped, "SYSTEM_CODE"),
        "facility_type_summary": summarize_group(clipped, "FACILITY_TYPE"),
        "top_named_segments": top_named_segments(clipped),
    }


def process_aoi(
    network: gpd.GeoDataFrame,
    spec: AoiSpec,
    output_root: Path,
    export_geojson: bool,
) -> dict[str, Any]:
    aoi_gdf = aoi_polygon(spec)
    clipped = gpd.overlay(network, aoi_gdf[["geometry"]], how="intersection")
    clipped = clipped.to_crs(TARGET_CRS)
    clipped = prepare_length_column(clipped)

    aoi_dir = output_root / spec.aoi_id
    aoi_dir.mkdir(parents=True, exist_ok=True)

    if len(clipped) > 0:
        clipped.to_file(
            aoi_dir / "network_clip.gpkg",
            layer="roadway_segments",
            driver="GPKG",
            engine="pyogrio",
        )
        if export_geojson:
            clipped.to_crs(4326).to_file(aoi_dir / "network_clip.geojson", driver="GeoJSON", engine="pyogrio")
    else:
        LOG.warning("AOI %s returned zero features.", spec.aoi_id)

    summary = build_summary(spec, clipped)
    (aoi_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    if summary["functional_class_summary"]:
        pd.DataFrame(summary["functional_class_summary"]).to_csv(
            aoi_dir / "functional_class_summary.csv",
            index=False,
        )
    if summary["system_code_summary"]:
        pd.DataFrame(summary["system_code_summary"]).to_csv(aoi_dir / "system_code_summary.csv", index=False)
    if summary["facility_type_summary"]:
        pd.DataFrame(summary["facility_type_summary"]).to_csv(
            aoi_dir / "facility_type_summary.csv",
            index=False,
        )

    write_review_template(aoi_dir / "manual_review_template.csv", spec, summary)
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Clip the staged base_network.gpkg to one or more AOIs and generate "
            "review artifacts for manual roadway-gap classification."
        )
    )
    parser.add_argument(
        "--gpkg-path",
        default=str(DEFAULT_GPKG),
        help="Path to the source GeoPackage containing roadway_segments.",
    )
    parser.add_argument("--layer", default=DEFAULT_LAYER, help="Layer name inside the GeoPackage.")
    parser.add_argument(
        "--output-root",
        default=str(DEFAULT_OUTPUT_ROOT),
        help="Directory where AOI review packages will be written.",
    )
    parser.add_argument(
        "--aoi-csv",
        help="CSV with columns aoi_id,xmin,ymin,xmax,ymax,srid,notes.",
    )
    parser.add_argument(
        "--bbox",
        nargs=4,
        type=float,
        metavar=("XMIN", "YMIN", "XMAX", "YMAX"),
        help="Single AOI bbox if not using --aoi-csv.",
    )
    parser.add_argument("--bbox-srid", type=int, default=4326, help="SRID for --bbox.")
    parser.add_argument("--aoi-id", help="Identifier for a single --bbox AOI.")
    parser.add_argument("--notes", help="Optional notes for a single --bbox AOI.")
    parser.add_argument(
        "--export-geojson",
        action="store_true",
        help="Also write a WGS84 GeoJSON clip for browser review.",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging.")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    configure_logging(args.verbose)

    aois = load_aois(args)
    network = load_network(Path(args.gpkg_path), args.layer).to_crs(TARGET_CRS)
    output_root = Path(args.output_root)
    output_root.mkdir(parents=True, exist_ok=True)

    summaries: list[dict[str, Any]] = []
    for spec in aois:
        LOG.info("Processing AOI %s", spec.aoi_id)
        summaries.append(process_aoi(network, spec, output_root, args.export_geojson))

    pd.DataFrame(
        [
            {
                "aoi_id": item["aoi_id"],
                "feature_count": item["feature_count"],
                "total_centerline_miles": item["total_centerline_miles"],
                "named_route_count": item["named_route_count"],
                "aadt_non_null_count": item["aadt_non_null_count"],
            }
            for item in summaries
        ]
    ).to_csv(output_root / "aoi_overview.csv", index=False)

    (output_root / "aoi_overview.json").write_text(json.dumps(summaries, indent=2), encoding="utf-8")
    LOG.info("Wrote AOI review package to %s", output_root)


if __name__ == "__main__":
    main()
