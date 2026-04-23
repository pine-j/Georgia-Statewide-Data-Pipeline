"""Stage definitions for the 01_roadway_inventory pipeline.

Each function is a thin wrapper that declares its fingerprint inputs
(upstream stages, raw files, config files, helper functions, globals)
and delegates to the existing helpers in normalize.py and its enrichment
modules.  The @stage infrastructure handles caching.
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]
SCRIPTS_DIR = REPO_ROOT / "02-Data-Staging" / "scripts" / "01_roadway_inventory"
sys.path.insert(0, str(SCRIPTS_DIR))

import geopandas as gpd
import pandas as pd

import normalize
from admin_breakpoints import (
    BoundaryCrosser,
    compute_route_crossings,
    resolve_segment_admin_attrs,
)
from evacuation_enrichment import apply_evacuation_enrichment, write_evacuation_summary
from hpms_enrichment import apply_hpms_enrichment, write_hpms_enrichment_summary
from rnhp_enrichment import (
    apply_speed_zone_enrichment,
    apply_off_system_speed_zone_enrichment,
    write_enrichment_summary,
)
from route_family import classify_route_families
from route_verification import (
    apply_signed_route_verification,
    write_signed_route_verification_summary,
)
from route_type_gdot import apply_gdot_route_type_classification
from utils import decode_lookup_value

from pipeline.stage import StageDefinition, StageRegistry

import logging

logger = logging.getLogger(__name__)

PIPELINE_DIR = Path(__file__).resolve().parent.parent
STAGE_FILE = Path(__file__).resolve()

RAW_DIR = REPO_ROOT / "01-Raw-Data" / "Roadway-Inventory"
CONFIG_DIR = REPO_ROOT / "02-Data-Staging" / "config"

_BOUNDARY_KEYS = [
    "county", "district", "state", "mpo", "regional_commission",
    "city", "area_office", "state_house", "state_senate", "congressional",
]


def _save_stage01_context(checkpoint_dir: Path, context: dict) -> None:
    """Persist current_traffic alongside the stage 01 checkpoint."""
    ct = context.get("current_traffic")
    if ct is not None:
        path = checkpoint_dir / "01_load_routes_traffic.parquet"
        ct.to_parquet(path, engine="pyarrow")


def _restore_stage01_context(checkpoint_dir: Path, context: dict) -> None:
    """Reload current_traffic on cache hit."""
    path = checkpoint_dir / "01_load_routes_traffic.parquet"
    if path.exists():
        context["current_traffic"] = pd.read_parquet(path)


def _save_stage02_context(checkpoint_dir: Path, context: dict) -> None:
    """Persist all boundary GeoDataFrames alongside the stage 02 checkpoint."""
    boundaries = context.get("boundaries", {})
    bdir = checkpoint_dir / "02_fetch_boundaries"
    bdir.mkdir(parents=True, exist_ok=True)
    for key in _BOUNDARY_KEYS:
        gdf = boundaries.get(key)
        if gdf is not None and not gdf.empty:
            gdf.to_parquet(bdir / f"{key}.parquet", engine="pyarrow")


def _restore_stage02_context(checkpoint_dir: Path, context: dict) -> None:
    """Reload all boundary GeoDataFrames on cache hit."""
    bdir = checkpoint_dir / "02_fetch_boundaries"
    boundaries = {}
    for key in _BOUNDARY_KEYS:
        path = bdir / f"{key}.parquet"
        if path.exists():
            boundaries[key] = gpd.read_parquet(path)
        else:
            logger.warning("Missing boundary checkpoint: %s", path)
            boundaries[key] = None
    context["boundaries"] = boundaries


def _find_raw_path(name: str) -> Path:
    return normalize.find_path(RAW_DIR, name)


registry = StageRegistry()


# ---------------------------------------------------------------------------
# Stage 01: load_routes
# ---------------------------------------------------------------------------

def _stage_01_load_routes(*, upstream_results, context):
    road_inv_gdb = _find_raw_path(normalize.ROAD_INV_GDB_NAME)
    traffic_gdb = _find_raw_path(normalize.TRAFFIC_GDB_NAME)

    routes = normalize.load_route_geometry(road_inv_gdb)
    routes = normalize.enrich_routes_with_static_attributes(routes, road_inv_gdb)

    current_traffic = normalize.load_current_traffic(traffic_gdb)
    routes = normalize.prepare_route_attributes(routes, current_traffic)
    routes = normalize.build_unique_id(routes)

    if routes.crs is not None and str(routes.crs) != normalize.TARGET_CRS:
        routes = routes.to_crs(normalize.TARGET_CRS)

    context["current_traffic"] = current_traffic
    return routes


registry.add(StageDefinition(
    name="01_load_routes",
    func=_stage_01_load_routes,
    context_save=_save_stage01_context,
    context_restore=_restore_stage01_context,
    raw_inputs=[
        RAW_DIR / normalize.ROAD_INV_GDB_NAME,
        RAW_DIR / "GDOT_Traffic" / "Traffic_2024_Geodatabase" / normalize.TRAFFIC_GDB_NAME,
    ],
    config_files=[
        CONFIG_DIR / "county_codes.json",
        CONFIG_DIR / "district_codes.json",
        CONFIG_DIR / "roadway_domain_labels.json",
    ],
    helpers=[
        normalize.load_route_geometry,
        normalize.clean_column_names,
        normalize.round_milepoint,
        normalize.enrich_routes_with_static_attributes,
        normalize.load_route_attribute_layer,
        normalize.load_current_traffic,
        normalize.prepare_route_attributes,
        normalize.parse_route_id,
        normalize.build_county_to_district_lookup,
        normalize.sync_derived_alias_fields,
        classify_route_families,
        normalize.build_unique_id,
    ],
    globals_list=[
        (normalize, "ROUTE_LAYER"),
        (normalize, "CURRENT_TRAFFIC_LAYER"),
        (normalize, "CURRENT_TRAFFIC_FIELDS"),
        (normalize, "ROUTE_ATTRIBUTE_LAYERS"),
        (normalize, "ROUTE_MERGE_KEYS"),
        (normalize, "MILEPOINT_PRECISION"),
        (normalize, "MILEPOINT_TOLERANCE"),
        (normalize, "DERIVED_ALIAS_SYNC_FIELDS"),
        (normalize, "TARGET_CRS"),
    ],
    code_files=[STAGE_FILE],
))


# ---------------------------------------------------------------------------
# Stage 02: fetch_boundaries
# ---------------------------------------------------------------------------

def _stage_02_fetch_boundaries(*, upstream_results, context):
    district_boundaries = normalize.fetch_official_district_boundaries()
    county_boundaries = normalize.fetch_official_county_boundaries(
        district_boundaries=district_boundaries
    )
    state_boundary = normalize.derive_state_boundary_from_counties(county_boundaries)
    mpo_boundaries = normalize.fetch_official_mpo_boundaries(state_boundary)
    regional_commission_boundaries = normalize.fetch_official_regional_commission_boundaries()
    city_boundaries = normalize.fetch_georgia_cities()
    area_office_boundaries = normalize.fetch_area_office_boundaries(
        county_boundaries,
        municipalities=city_boundaries,
    )
    state_house_boundaries = normalize.fetch_state_house_boundaries()
    state_senate_boundaries = normalize.fetch_state_senate_boundaries()
    congressional_boundaries = normalize.fetch_congressional_boundaries()

    context["boundaries"] = {
        "county": county_boundaries,
        "district": district_boundaries,
        "state": state_boundary,
        "mpo": mpo_boundaries,
        "regional_commission": regional_commission_boundaries,
        "city": city_boundaries,
        "area_office": area_office_boundaries,
        "state_house": state_house_boundaries,
        "state_senate": state_senate_boundaries,
        "congressional": congressional_boundaries,
    }
    return county_boundaries


registry.add(StageDefinition(
    name="02_fetch_boundaries",
    context_save=_save_stage02_context,
    context_restore=_restore_stage02_context,
    func=_stage_02_fetch_boundaries,
    produces_geodataframe=True,
    raw_inputs=[],
    config_files=[
        CONFIG_DIR / "area_office_codes.json",
    ],
    helpers=[
        normalize.fetch_official_district_boundaries,
        normalize.fetch_official_county_boundaries,
        normalize.derive_state_boundary_from_counties,
        normalize.fetch_official_mpo_boundaries,
        normalize.fetch_official_regional_commission_boundaries,
        normalize.fetch_georgia_cities,
        normalize.fetch_area_office_boundaries,
        normalize.fetch_state_house_boundaries,
        normalize.fetch_state_senate_boundaries,
        normalize.fetch_congressional_boundaries,
        normalize.fetch_and_cache_boundary,
        normalize._repair_boundary_geometry,
        normalize._read_tiger_zip,
        normalize._read_tiger_zip_from_file,
        normalize._resolve_boundary_local_cache,
        normalize._assign_city_id,
        normalize._load_area_office_codes,
        normalize._derive_area_office_polygons,
        normalize._build_subcounty_split_geoms,
        normalize._municipality_name_col,
        normalize.clean_column_names,
    ],
    globals_list=[
        (normalize, "DISTRICT_BOUNDARIES_URL"),
        (normalize, "COUNTY_BOUNDARIES_URL"),
        (normalize, "MPO_BOUNDARIES_URL"),
        (normalize, "REGIONAL_COMMISSION_BOUNDARIES_URL"),
        (normalize, "GEORGIA_CITIES_URL"),
        (normalize, "STATE_HOUSE_BOUNDARIES_URL"),
        (normalize, "STATE_SENATE_BOUNDARIES_URL"),
        (normalize, "CONGRESSIONAL_BOUNDARIES_URL"),
        (normalize, "TARGET_CRS"),
        (normalize, "CONFIG_DIR"),
        (normalize, "REBUILD_OUTPUTS_DIR"),
        (normalize, "RAW_BOUNDARIES_CACHE_DIR"),
        (normalize, "DISTRICT_SHORT_NAME_LOOKUP"),
        (normalize, "CITY_ID_OBJECTID_OFFSET"),
        (normalize, "TIGER_YEAR"),
        (normalize, "GEORGIA_STATE_FIPS"),
    ],
    code_files=[STAGE_FILE],
))


# ---------------------------------------------------------------------------
# Stage 03: write_admin_snapshots
# ---------------------------------------------------------------------------

def _stage_03_write_admin_snapshots(*, upstream_results, context):
    boundaries = context["boundaries"]
    normalize.write_admin_code_snapshots(
        mpo_boundaries=boundaries["mpo"],
        regional_commission_boundaries=boundaries["regional_commission"],
    )
    return None


registry.add(StageDefinition(
    name="03_write_admin_snapshots",
    func=_stage_03_write_admin_snapshots,
    upstream=["02_fetch_boundaries"],
    produces_geodataframe=False,
    helpers=[
        normalize.write_admin_code_snapshots,
        normalize._write_admin_code_snapshot,
    ],
    globals_list=[
        (normalize, "CONFIG_DIR"),
    ],
    code_files=[STAGE_FILE],
))


# ---------------------------------------------------------------------------
# Stage 04: segment
# ---------------------------------------------------------------------------

def _stage_04_segment(*, upstream_results, context):
    routes = upstream_results["01_load_routes"].data
    boundaries = context["boundaries"]
    current_traffic = context["current_traffic"]

    crossers = [
        BoundaryCrosser(
            name="county",
            gdf=boundaries["county"],
            attribute_cols={"COUNTYFP": "COUNTY_CODE", "NAME": "COUNTY_NAME"},
        ),
        BoundaryCrosser(
            name="district",
            gdf=boundaries["district"],
            attribute_cols={
                "GDOT_DISTRICT": "DISTRICT",
                "DISTRICT_NAME": "DISTRICT_NAME",
            },
        ),
        BoundaryCrosser(
            name="area_office",
            gdf=boundaries["area_office"],
            attribute_cols={
                "AREA_OFFICE_ID": "AREA_OFFICE_ID",
                "AREA_OFFICE_NAME": "AREA_OFFICE_NAME",
            },
        ),
        BoundaryCrosser(
            name="mpo",
            gdf=boundaries["mpo"],
            attribute_cols={"MPO_ID": "MPO_ID", "MPO_NAME": "MPO_NAME"},
        ),
        BoundaryCrosser(
            name="regional_commission",
            gdf=boundaries["regional_commission"],
            attribute_cols={"RC_ID": "RC_ID", "RC_NAME": "RC_NAME"},
        ),
    ]

    current_lookup = normalize.build_interval_lookup(current_traffic)
    current_lookup = normalize.mirror_inc_breakpoints_to_dec(
        current_lookup,
        routes["ROUTE_ID"].dropna().astype(str).unique().tolist(),
    )
    segmented = normalize.segment_routes(routes, current_lookup, crossers=crossers)
    segmented = normalize.build_unique_id(segmented)
    segmented = normalize.apply_unique_id_collision_guard(segmented)

    if segmented.crs is not None and str(segmented.crs) != normalize.TARGET_CRS:
        segmented = segmented.to_crs(normalize.TARGET_CRS)

    return segmented


registry.add(StageDefinition(
    name="04_segment",
    func=_stage_04_segment,
    upstream=["01_load_routes", "02_fetch_boundaries"],
    helpers=[
        normalize.build_interval_lookup,
        normalize.mirror_inc_breakpoints_to_dec,
        normalize.segment_routes,
        normalize.build_segment_row,
        normalize.prepare_route_geometry_components,
        normalize._extract_route_line_components,
        normalize.find_covering_record,
        normalize.get_breakpoints,
        normalize.clamp_interval,
        normalize.slice_route_geometry,
        normalize.compute_truck_pct,
        normalize._clean_optional_text,
        compute_route_crossings,
        resolve_segment_admin_attrs,
        normalize.build_unique_id,
        normalize.apply_unique_id_collision_guard,
    ],
    globals_list=[
        (normalize, "MILEPOINT_TOLERANCE"),
        (normalize, "UNIQUE_ID_COLLISION_ADMIN_COLS"),
        (normalize, "TARGET_CRS"),
    ],
    code_files=[STAGE_FILE],
))


# ---------------------------------------------------------------------------
# Stage 05: admin_overlay_flags_and_length
# ---------------------------------------------------------------------------

def _stage_05_admin_overlay_flags_and_length(*, upstream_results, context):
    segmented = upstream_results["04_segment"].data
    boundaries = context["boundaries"]

    segmented = normalize.apply_admin_overlay_flags(
        segmented,
        house_boundaries=boundaries["state_house"],
        senate_boundaries=boundaries["state_senate"],
        congressional_boundaries=boundaries["congressional"],
        city_boundaries=boundaries["city"],
    )
    segmented = normalize.compute_segment_length(segmented)
    return segmented


registry.add(StageDefinition(
    name="05_admin_overlay_flags_and_length",
    func=_stage_05_admin_overlay_flags_and_length,
    upstream=["04_segment", "02_fetch_boundaries"],
    helpers=[
        normalize.apply_admin_overlay_flags,
        normalize._overlay_winner_by_length,
        normalize._assign_city_id,
        normalize.compute_segment_length,
    ],
    code_files=[STAGE_FILE],
))


# ---------------------------------------------------------------------------
# Stage 06: speed_zone_enrichment
# ---------------------------------------------------------------------------

def _stage_06_speed_zone_enrichment(*, upstream_results, context):
    segmented = upstream_results["05_admin_overlay_flags_and_length"].data
    segmented = apply_speed_zone_enrichment(segmented)
    return segmented


registry.add(StageDefinition(
    name="06_speed_zone_enrichment",
    func=_stage_06_speed_zone_enrichment,
    upstream=["05_admin_overlay_flags_and_length"],
    helpers=[
        apply_speed_zone_enrichment,
    ],
    code_files=[
        STAGE_FILE,
        SCRIPTS_DIR / "rnhp_enrichment.py",
    ],
))


# ---------------------------------------------------------------------------
# Stage 07: county_district_backfill
# ---------------------------------------------------------------------------

def _stage_07_county_district_backfill(*, upstream_results, context):
    segmented = upstream_results["06_speed_zone_enrichment"].data
    county_boundaries = context["boundaries"]["county"]
    segmented = normalize.backfill_county_district_from_geometry(
        segmented,
        county_boundaries,
    )
    return segmented


registry.add(StageDefinition(
    name="07_county_district_backfill",
    func=_stage_07_county_district_backfill,
    upstream=["06_speed_zone_enrichment", "02_fetch_boundaries"],
    helpers=[
        normalize.backfill_county_district_from_geometry,
        normalize._prepare_county_boundaries_for_spatial_use,
        normalize._normalized_county_code_series,
        normalize._assign_majority_county_district,
        normalize._overlay_segment_county_lengths,
    ],
    globals_list=[
        (normalize, "MILEPOINT_TOLERANCE"),
        (normalize, "TARGET_CRS"),
    ],
    code_files=[STAGE_FILE],
))


# ---------------------------------------------------------------------------
# Stage 08: hpms_enrichment
# ---------------------------------------------------------------------------

def _stage_08_hpms_enrichment(*, upstream_results, context):
    segmented = upstream_results["07_county_district_backfill"].data
    segmented = apply_hpms_enrichment(segmented)
    return segmented


registry.add(StageDefinition(
    name="08_hpms_enrichment",
    func=_stage_08_hpms_enrichment,
    upstream=["07_county_district_backfill"],
    raw_inputs=[
        RAW_DIR / "FHWA_HPMS",
    ],
    helpers=[
        apply_hpms_enrichment,
    ],
    code_files=[
        STAGE_FILE,
        SCRIPTS_DIR / "hpms_enrichment.py",
    ],
))


# ---------------------------------------------------------------------------
# Stage 09: aadt_2024_source_agreement
# ---------------------------------------------------------------------------

def _stage_09_aadt_2024_source_agreement(*, upstream_results, context):
    segmented = upstream_results["08_hpms_enrichment"].data
    segmented = normalize.compute_aadt_2024_source_agreement(segmented)
    return segmented


registry.add(StageDefinition(
    name="09_aadt_2024_source_agreement",
    func=_stage_09_aadt_2024_source_agreement,
    upstream=["08_hpms_enrichment"],
    helpers=[
        normalize.compute_aadt_2024_source_agreement,
    ],
    globals_list=[
        (normalize, "AADT_2024_AGREEMENT_ABS_TOL"),
        (normalize, "AADT_2024_AGREEMENT_REL_TOL"),
    ],
    code_files=[STAGE_FILE],
))


# ---------------------------------------------------------------------------
# Stage 10: off_system_speed_zone_enrichment
# ---------------------------------------------------------------------------

def _stage_10_off_system_speed_zone_enrichment(*, upstream_results, context):
    segmented = upstream_results["09_aadt_2024_source_agreement"].data
    segmented = apply_off_system_speed_zone_enrichment(segmented)
    return segmented


registry.add(StageDefinition(
    name="10_off_system_speed_zone_enrichment",
    func=_stage_10_off_system_speed_zone_enrichment,
    upstream=["09_aadt_2024_source_agreement"],
    helpers=[
        apply_off_system_speed_zone_enrichment,
    ],
    code_files=[
        STAGE_FILE,
        SCRIPTS_DIR / "rnhp_enrichment.py",
    ],
))


# ---------------------------------------------------------------------------
# Stage 11: signed_route_verification
# ---------------------------------------------------------------------------

def _stage_11_signed_route_verification(*, upstream_results, context):
    segmented = upstream_results["10_off_system_speed_zone_enrichment"].data
    segmented = apply_signed_route_verification(segmented)
    return segmented


registry.add(StageDefinition(
    name="11_signed_route_verification",
    func=_stage_11_signed_route_verification,
    upstream=["10_off_system_speed_zone_enrichment"],
    helpers=[
        apply_signed_route_verification,
    ],
    code_files=[
        STAGE_FILE,
        SCRIPTS_DIR / "route_verification.py",
        SCRIPTS_DIR / "arcgis_client.py",
    ],
))


# ---------------------------------------------------------------------------
# Stage 12: route_type_gdot
# ---------------------------------------------------------------------------

def _stage_12_route_type_gdot(*, upstream_results, context):
    segmented = upstream_results["11_signed_route_verification"].data
    route_type_fields = apply_gdot_route_type_classification(segmented)
    segmented = pd.concat([segmented, route_type_fields], axis=1)
    return segmented


registry.add(StageDefinition(
    name="12_route_type_gdot",
    func=_stage_12_route_type_gdot,
    upstream=["11_signed_route_verification"],
    helpers=[
        apply_gdot_route_type_classification,
    ],
    code_files=[
        STAGE_FILE,
        SCRIPTS_DIR / "route_type_gdot.py",
    ],
))


# ---------------------------------------------------------------------------
# Stage 13: evacuation_enrichment
# ---------------------------------------------------------------------------

def _stage_13_evacuation_enrichment(*, upstream_results, context):
    segmented = upstream_results["12_route_type_gdot"].data
    segmented = apply_evacuation_enrichment(segmented)
    segmented = normalize.build_unique_id(segmented)
    segmented = normalize.apply_unique_id_collision_guard(segmented)
    return segmented


registry.add(StageDefinition(
    name="13_evacuation_enrichment",
    func=_stage_13_evacuation_enrichment,
    upstream=["12_route_type_gdot"],
    raw_inputs=[
        RAW_DIR / "GDOT_EOC",
    ],
    helpers=[
        apply_evacuation_enrichment,
        normalize.build_unique_id,
        normalize.apply_unique_id_collision_guard,
    ],
    globals_list=[
        (normalize, "UNIQUE_ID_COLLISION_ADMIN_COLS"),
    ],
    code_files=[
        STAGE_FILE,
        SCRIPTS_DIR / "evacuation_enrichment.py",
        SCRIPTS_DIR / "_evac_corridor_match.py",
    ],
))


# ---------------------------------------------------------------------------
# Stage 14: aadt_gap_fill_and_labels
# ---------------------------------------------------------------------------

def _stage_14_aadt_gap_fill_and_labels(*, upstream_results, context):
    segmented = upstream_results["13_evacuation_enrichment"].data
    county_boundaries = context["boundaries"]["county"]

    segmented = normalize.sync_derived_alias_fields(segmented)
    segmented = normalize.apply_direction_mirror_aadt(segmented)
    segmented = normalize.apply_state_system_current_aadt_gap_fill(segmented)
    segmented = normalize.apply_nearest_neighbor_aadt(segmented)
    segmented = normalize.apply_future_aadt_fill_chain(segmented)
    segmented = normalize.apply_future_aadt_official_growth_projection(segmented)
    segmented = normalize.recompute_aadt_2024_confidence(segmented)
    segmented = normalize.derive_texas_alignment_columns(segmented)
    segmented = normalize.add_decoded_label_columns(segmented)
    segmented = normalize.add_county_all_from_geometry(segmented, county_boundaries)
    segmented = normalize._move_column_after(segmented, "county_all", "COUNTY_NAME")
    return segmented


registry.add(StageDefinition(
    name="14_aadt_gap_fill_and_labels",
    func=_stage_14_aadt_gap_fill_and_labels,
    upstream=["13_evacuation_enrichment", "02_fetch_boundaries"],
    helpers=[
        normalize.sync_derived_alias_fields,
        normalize.apply_direction_mirror_aadt,
        normalize.apply_state_system_current_aadt_gap_fill,
        normalize.apply_nearest_neighbor_aadt,
        normalize.apply_future_aadt_fill_chain,
        normalize.apply_future_aadt_official_growth_projection,
        normalize.recompute_aadt_2024_confidence,
        normalize.derive_texas_alignment_columns,
        normalize.add_decoded_label_columns,
        normalize._apply_geometry_wins_label,
        normalize.get_or_empty_series,
        decode_lookup_value,
        normalize.add_county_all_from_geometry,
        normalize._prepare_county_boundaries_for_spatial_use,
        normalize._overlay_segment_county_lengths,
        normalize._dedupe_county_names,
        normalize._merge_county_all_value,
        normalize._clean_optional_text,
        normalize._move_column_after,
    ],
    globals_list=[
        (normalize, "DERIVED_ALIAS_SYNC_FIELDS"),
        (normalize, "MILEPOINT_TOLERANCE"),
        (normalize, "AADT_2024_GAP_FILL_MAX_INTERPOLATION_MILES"),
        (normalize, "NEAREST_NEIGHBOR_MAX_DISTANCE_MI"),
        (normalize, "_AADT_2024_DERIVED_SOURCES"),
        (normalize, "COUNTY_NAME_LOOKUP"),
        (normalize, "DISTRICT_SHORT_NAME_LOOKUP"),
        (normalize, "ROADWAY_DOMAIN_LABELS"),
        (normalize, "COUNTY_ALL_MIN_SHARE"),
        (normalize, "COUNTY_ALL_DELIMITER"),
    ],
    config_files=[
        CONFIG_DIR / "county_codes.json",
        CONFIG_DIR / "district_codes.json",
        CONFIG_DIR / "roadway_domain_labels.json",
    ],
    code_files=[STAGE_FILE],
))


# ---------------------------------------------------------------------------
# Stage 15: publish
# ---------------------------------------------------------------------------

def _stage_15_publish(*, upstream_results, context):
    from shapely import force_2d

    segmented = upstream_results["14_aadt_gap_fill_and_labels"].data
    boundaries = context["boundaries"]
    county_boundaries = boundaries["county"]

    normalize.logger.info("Final segment count: %d", len(segmented))
    normalize.logger.info(
        "Current AADT official coverage: %d segments",
        segmented["AADT_2024_OFFICIAL"].notna().sum(),
    )
    normalize.logger.info(
        "Current AADT final coverage: %d segments",
        segmented["AADT"].notna().sum(),
    )
    if "FUTURE_AADT_2044" in segmented.columns:
        normalize.logger.info(
            "Future AADT 2044 coverage: %d segments",
            segmented["FUTURE_AADT_2044"].notna().sum(),
        )

    cols_to_drop = [
        c for c in normalize.COLUMNS_TO_DROP_FROM_OUTPUT if c in segmented.columns
    ]
    if cols_to_drop:
        segmented = segmented.drop(columns=cols_to_drop)

    normalize.TABLES_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = normalize.TABLES_DIR / "roadway_inventory_cleaned.csv"
    segmented.drop(columns=["geometry"], errors="ignore").to_csv(csv_path, index=False)
    normalize.logger.info("Wrote staged roadway table CSV: %s (%d rows)", csv_path, len(segmented))

    segmented["geometry"] = segmented["geometry"].apply(
        lambda geom: force_2d(geom) if geom is not None else geom
    )

    normalize.SPATIAL_DIR.mkdir(parents=True, exist_ok=True)
    gpkg_path = normalize.SPATIAL_DIR / "base_network.gpkg"
    fallback_county, fallback_district = normalize.load_existing_boundary_layers(gpkg_path)
    if gpkg_path.exists():
        gpkg_path.unlink()
    segmented.to_file(gpkg_path, layer="roadway_segments", driver="GPKG", engine="pyogrio")

    staged_county, _ = normalize.write_supporting_boundary_layers(
        gpkg_path,
        fallback_county_boundaries=fallback_county,
        fallback_district_boundaries=fallback_district,
        county_boundaries=county_boundaries,
        district_boundaries=boundaries["district"],
        area_office_boundaries=boundaries["area_office"],
        mpo_boundaries=boundaries["mpo"],
        regional_commission_boundaries=boundaries["regional_commission"],
        state_house_boundaries=boundaries["state_house"],
        state_senate_boundaries=boundaries["state_senate"],
        congressional_boundaries=boundaries["congressional"],
    )
    normalize.assert_decoded_county_lookup_matches_boundaries(segmented, staged_county)

    normalize.write_match_summary(segmented)
    normalize.write_current_aadt_coverage_audit(segmented)
    normalize.write_enrichment_summary(segmented)
    normalize.write_hpms_enrichment_summary(segmented)
    normalize.write_signed_route_verification_summary(segmented)
    normalize.write_evacuation_summary(segmented)

    normalize.logger.info("Normalization complete.")
    return None


registry.add(StageDefinition(
    name="15_publish",
    func=_stage_15_publish,
    upstream=["14_aadt_gap_fill_and_labels", "02_fetch_boundaries"],
    produces_geodataframe=False,
    helpers=[
        normalize.load_existing_boundary_layers,
        normalize.write_supporting_boundary_layers,
        normalize._append_boundary_layer,
        normalize._load_boundary_from_rebuild_cache,
        normalize.assert_decoded_county_lookup_matches_boundaries,
        normalize.write_match_summary,
        normalize.write_current_aadt_coverage_audit,
        normalize._group_current_aadt_coverage,
        normalize.build_state_system_gap_fill_candidates,
        write_enrichment_summary,
        write_hpms_enrichment_summary,
        write_signed_route_verification_summary,
        write_evacuation_summary,
    ],
    globals_list=[
        (normalize, "TABLES_DIR"),
        (normalize, "SPATIAL_DIR"),
        (normalize, "REPORTS_DIR"),
        (normalize, "REBUILD_OUTPUTS_DIR"),
        (normalize, "CURRENT_AADT_AUDIT_DIR"),
        (normalize, "COLUMNS_TO_DROP_FROM_OUTPUT"),
        (normalize, "TARGET_CRS"),
    ],
    code_files=[
        STAGE_FILE,
        SCRIPTS_DIR / "rnhp_enrichment.py",
        SCRIPTS_DIR / "hpms_enrichment.py",
        SCRIPTS_DIR / "route_verification.py",
        SCRIPTS_DIR / "evacuation_enrichment.py",
    ],
))
