"""Georgia SocioEconomic data category for the RAPTOR pipeline.

Unlike Texas (which uses SAMv5 TAZ zones with projected demographics),
Georgia uses Census block groups as the base geography.  Current population
and employment come from ACS 5-Year estimates and LEHD LODES, respectively.
Future projections (2050) are derived by applying:

  - OPB county-level population growth factors to block-group base population
  - BLS/DOL-style county employment growth factors to block-group employment

Methodology:
  1. Buffer each roadway segment by 1 mile
  2. Overlay with block groups (area-weighted intersection)
  3. Aggregate population and employment within each buffer
  4. Compute density (per square mile) for current and 2050 horizons
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

from .Roadways import RoadwayData

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[4]
DB_PATH = PROJECT_ROOT / "03-Processed-Data" / "demographics" / "socioeconomic.db"
GPKG_PATH = PROJECT_ROOT / "03-Processed-Data" / "demographics" / "demographics.gpkg"
CLEAN_DIR = PROJECT_ROOT / "02-Data-Staging" / "cleaned" / "demographics"

TARGET_CRS = "EPSG:32617"
METERS_PER_MILE = 1_609.34
SQ_METERS_PER_SQ_MILE = METERS_PER_MILE ** 2

# Default projection year
PROJECTION_YEAR = 2050


class SocioEconomic:
    """Compute socioeconomic density metrics for Georgia roadway segments.

    Attributes:
        POPULATION_DENSITY: Current population density (per sq mi).
        EMPLOYMENT_DENSITY: Current employment density (per sq mi).
        POPULATION_DENSITY_2050: Projected 2050 population density.
        EMPLOYMENT_DENSITY_2050: Projected 2050 employment density.
        SOCIOECONOMY_SCORE: Composite socioeconomic needs score.
    """

    POPULATION_DENSITY = "Population_Density"
    EMPLOYMENT_DENSITY = "Employment_Density"
    POPULATION_DENSITY_2050 = "Population_Density_2050"
    EMPLOYMENT_DENSITY_2050 = "Employment_Density_2050"
    SOCIOECONOMY_SCORE = "Socio_Economic_Needs_Score"

    def __init__(self, year: int = 2024):
        self.year = year

        self.block_groups: gpd.GeoDataFrame | None = None
        self.lodes_wac: pd.DataFrame | None = None
        self.opb_projections: pd.DataFrame | None = None

        self.weights = {
            self.POPULATION_DENSITY: 0.25,
            self.EMPLOYMENT_DENSITY: 0.25,
            self.POPULATION_DENSITY_2050: 0.25,
            self.EMPLOYMENT_DENSITY_2050: 0.25,
        }

    # ------------------------------------------------------------------
    # Data loading
    # ------------------------------------------------------------------

    def load_data(self) -> None:
        """Load block-group geometry, LODES employment, and OPB projections."""
        # Block groups with ACS attributes (from GeoPackage or cleaned GPKG)
        if GPKG_PATH.exists():
            self.block_groups = gpd.read_file(GPKG_PATH, layer="block_groups")
            self.block_groups = self.block_groups.to_crs(TARGET_CRS)
            logger.info("Loaded %d block groups from GeoPackage", len(self.block_groups))
        else:
            acs_gpkg = CLEAN_DIR / "acs_block_groups.gpkg"
            if acs_gpkg.exists():
                self.block_groups = gpd.read_file(acs_gpkg)
                self.block_groups = self.block_groups.to_crs(TARGET_CRS)
                logger.info("Loaded %d block groups from cleaned GPKG", len(self.block_groups))
            else:
                raise FileNotFoundError(
                    f"No block group geometry found. Expected {GPKG_PATH} or {acs_gpkg}."
                )

        # Ensure GEOID is present and zero-padded
        if "GEOID" in self.block_groups.columns:
            self.block_groups["GEOID"] = self.block_groups["GEOID"].astype(str).str.zfill(12)

        # Compute block-group area in square miles (from projected geometry)
        self.block_groups["area_sq_mi"] = (
            self.block_groups.geometry.area / SQ_METERS_PER_SQ_MILE
        )

        # LODES WAC for employment (aggregated to block group)
        self._load_lodes_employment()

        # OPB county growth factors
        self._load_opb_growth_factors()

    def _load_lodes_employment(self) -> None:
        """Load LODES WAC data and aggregate jobs to block-group level."""
        wac_csv = CLEAN_DIR / "lodes_wac.csv"
        if not wac_csv.exists():
            logger.warning("LODES WAC not found at %s", wac_csv)
            return

        wac = pd.read_csv(wac_csv, dtype={"w_geocode": str, "BLOCK_GROUP_GEOID": str})

        if "C000" in wac.columns:
            wac["C000"] = pd.to_numeric(wac["C000"], errors="coerce").fillna(0)
        else:
            logger.warning("C000 (total jobs) column not found in LODES WAC")
            return

        if "BLOCK_GROUP_GEOID" not in wac.columns and "w_geocode" in wac.columns:
            wac["BLOCK_GROUP_GEOID"] = wac["w_geocode"].str[:12]

        # Aggregate total jobs by block group
        self.lodes_wac = (
            wac.groupby("BLOCK_GROUP_GEOID")
            .agg(total_jobs=("C000", "sum"))
            .reset_index()
        )
        logger.info("LODES employment: %d block groups", len(self.lodes_wac))

    def _load_opb_growth_factors(self) -> None:
        """Load OPB population projections and compute county growth factors.

        The OPB publishes county-level population for multiple future years.
        We compute a growth factor = projected_2050 / base_2020 for each county
        and apply it to block-group base population.
        """
        opb_csv = CLEAN_DIR / "opb_projections.csv"
        if not opb_csv.exists():
            logger.warning("OPB projections not found at %s", opb_csv)
            return

        opb = pd.read_csv(opb_csv)

        # OPB format varies; look for year columns or a 'Year' row layout
        # Common pattern: county name in first column, year values across columns
        year_cols = [c for c in opb.columns if c.isdigit()]
        if year_cols and "2020" in year_cols and "2050" in year_cols:
            opb["pop_growth_factor"] = (
                pd.to_numeric(opb["2050"], errors="coerce")
                / pd.to_numeric(opb["2020"], errors="coerce")
            ).fillna(1.0)
        else:
            logger.warning(
                "Could not identify year columns in OPB data; "
                "using default 1.15 growth factor"
            )
            opb["pop_growth_factor"] = 1.15

        self.opb_projections = opb
        logger.info("OPB growth factors loaded: %d counties", len(opb))

    # ------------------------------------------------------------------
    # Metric computation
    # ------------------------------------------------------------------

    def get_employment_population_metrics(
        self,
        roadways: RoadwayData,
        buffer_radius_miles: float = 1.0,
    ) -> None:
        """Compute population and employment density metrics for each segment.

        Steps:
        1. Buffer each roadway segment by *buffer_radius_miles*
        2. Overlay with block groups (intersection)
        3. Area-weight population and employment from overlapping block groups
        4. Compute density = total / buffer area
        5. Apply OPB growth factors for 2050 projection

        Parameters
        ----------
        roadways : RoadwayData
            Loaded Georgia roadway inventory (must have GA_RDWY_INV).
        buffer_radius_miles : float
            Buffer radius in miles (default 1.0).
        """
        if self.block_groups is None:
            raise RuntimeError("Call load_data() before computing metrics.")

        rdwy = roadways.GA_RDWY_INV
        if rdwy is None or rdwy.empty:
            raise RuntimeError("Roadway data is empty.")

        logger.info(
            "Computing socioeconomic metrics (buffer=%.1f mi) for %d segments",
            buffer_radius_miles,
            len(rdwy),
        )

        # --- Prepare block group attributes ---
        bg = self.block_groups.copy()

        # Population from ACS
        pop_col = "B01003_001E"
        if pop_col in bg.columns:
            bg["population"] = pd.to_numeric(bg[pop_col], errors="coerce").fillna(0)
        else:
            bg["population"] = 0
            logger.warning("ACS population column %s not found", pop_col)

        # Employment from LODES (joined by GEOID)
        bg["employment"] = 0
        if self.lodes_wac is not None and "GEOID" in bg.columns:
            emp_map = self.lodes_wac.set_index("BLOCK_GROUP_GEOID")["total_jobs"]
            bg["employment"] = bg["GEOID"].map(emp_map).fillna(0)

        # County FIPS for growth factors
        if "GEOID" in bg.columns:
            bg["COUNTY_FIPS"] = bg["GEOID"].str[:5]

        # --- Buffer roadway segments ---
        buffer_m = buffer_radius_miles * METERS_PER_MILE
        buffers = gpd.GeoDataFrame(
            {"unique_id": rdwy["unique_id"], "geometry": rdwy.geometry.buffer(buffer_m)},
            crs=TARGET_CRS,
        )
        buffers["buffer_area_sq_mi"] = buffers.geometry.area / SQ_METERS_PER_SQ_MILE

        # --- Overlay: intersection of buffers with block groups ---
        intersections = gpd.overlay(buffers, bg, how="intersection")

        # Area-weight: proportion of block group covered by the buffer
        intersections["intersect_area"] = intersections.geometry.area
        intersections["bg_area"] = intersections["area_sq_mi"] * SQ_METERS_PER_SQ_MILE
        intersections["weight"] = (
            intersections["intersect_area"] / intersections["bg_area"]
        ).clip(0, 1)

        intersections["weighted_pop"] = intersections["population"] * intersections["weight"]
        intersections["weighted_emp"] = intersections["employment"] * intersections["weight"]

        # --- Aggregate by segment ---
        agg = (
            intersections.groupby("unique_id")
            .agg(
                total_pop=("weighted_pop", "sum"),
                total_emp=("weighted_emp", "sum"),
                buffer_area_sq_mi=("buffer_area_sq_mi", "first"),
            )
            .reset_index()
        )

        # Current densities
        agg[self.POPULATION_DENSITY] = (agg["total_pop"] / agg["buffer_area_sq_mi"]).round(2)
        agg[self.EMPLOYMENT_DENSITY] = (agg["total_emp"] / agg["buffer_area_sq_mi"]).round(2)

        # --- 2050 Projections ---
        pop_growth = self._get_segment_growth_factors(intersections)
        emp_growth_factor = 1.10  # Default BLS/DOL-style factor for Georgia

        agg_2050 = (
            intersections.assign(
                weighted_pop_2050=lambda x: (
                    x["weighted_pop"]
                    * x["COUNTY_FIPS"].map(pop_growth).fillna(1.15)
                ),
                weighted_emp_2050=lambda x: x["weighted_emp"] * emp_growth_factor,
            )
            .groupby("unique_id")
            .agg(
                total_pop_2050=("weighted_pop_2050", "sum"),
                total_emp_2050=("weighted_emp_2050", "sum"),
                buffer_area_sq_mi=("buffer_area_sq_mi", "first"),
            )
            .reset_index()
        )

        agg[self.POPULATION_DENSITY_2050] = (
            agg_2050["total_pop_2050"] / agg_2050["buffer_area_sq_mi"]
        ).round(2)
        agg[self.EMPLOYMENT_DENSITY_2050] = (
            agg_2050["total_emp_2050"] / agg_2050["buffer_area_sq_mi"]
        ).round(2)

        # --- Merge back to roadways ---
        metric_cols = [
            "unique_id",
            self.POPULATION_DENSITY,
            self.EMPLOYMENT_DENSITY,
            self.POPULATION_DENSITY_2050,
            self.EMPLOYMENT_DENSITY_2050,
        ]
        roadways.GA_RDWY_INV = rdwy.merge(agg[metric_cols], on="unique_id", how="left")

        # Fill segments with no overlap
        for col in metric_cols[1:]:
            roadways.GA_RDWY_INV[col] = roadways.GA_RDWY_INV[col].fillna(0)

        logger.info("Socioeconomic metrics computed for %d segments", len(roadways.GA_RDWY_INV))

    def _get_segment_growth_factors(
        self,
        intersections: gpd.GeoDataFrame,
    ) -> pd.Series:
        """Build a county FIPS -> population growth factor mapping.

        Uses OPB projections if available, otherwise returns a default factor.
        """
        default_factor = 1.15  # ~15% growth 2020-2050 statewide average

        if self.opb_projections is None or "pop_growth_factor" not in self.opb_projections.columns:
            logger.info("Using default population growth factor: %.2f", default_factor)
            counties = intersections["COUNTY_FIPS"].unique()
            return pd.Series(default_factor, index=counties)

        # Try to map OPB county names to FIPS codes
        opb = self.opb_projections
        if "COUNTY_FIPS" in opb.columns:
            return opb.set_index("COUNTY_FIPS")["pop_growth_factor"]

        # Fallback: use statewide average from OPB
        avg_growth = opb["pop_growth_factor"].mean()
        logger.info("Using OPB statewide average growth factor: %.3f", avg_growth)
        counties = intersections["COUNTY_FIPS"].unique()
        return pd.Series(avg_growth, index=counties)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def clear_data(self) -> None:
        """Release loaded data from memory."""
        self.block_groups = None
        self.lodes_wac = None
        self.opb_projections = None
        logger.info("Cleared socioeconomic data")

    def generate_metrics(self, roadways: RoadwayData) -> None:
        """Full pipeline: load data, compute metrics, free memory."""
        self.load_data()
        self.get_employment_population_metrics(roadways)
        self.clear_data()
        logger.info("Socioeconomic metrics generation complete")
