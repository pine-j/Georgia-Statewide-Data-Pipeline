"""SRP Priority Route matching for Georgia road segments.

Loads the cleaned SRP (Statewide Strategic Route Plan) priority route data
and determines whether each road segment lies on a Critical or High priority
route via spatial overlay.
"""

from pathlib import Path

import geopandas as gpd
import numpy as np
from shapely.geometry import LineString, MultiLineString

TARGET_CRS = "EPSG:32617"


class PriorityRoutes:
    """Match road segments to GDOT SRP priority routes."""

    IS_SRP_CRITICAL_OR_HIGH = "Is_Seg_On_SRP_Critical_or_High"

    _CLEAN_DIR = Path("02-Data-Staging") / "cleaned" / "connectivity"

    def __init__(self, project_root: Path | None = None):
        self._root = project_root or Path(__file__).resolve().parents[5]
        self._clean_path = (
            self._root / self._CLEAN_DIR / "priority_routes.geojson"
        )
        self.priority_routes: gpd.GeoDataFrame | None = None

    # ------------------------------------------------------------------
    # Data loading
    # ------------------------------------------------------------------

    def load_data(self) -> None:
        """Load cleaned SRP priority route data."""
        print("Loading SRP priority routes ...")
        if not self._clean_path.exists():
            print(f"  WARNING: {self._clean_path} not found.")
            return
        gdf = gpd.read_file(self._clean_path)
        if gdf.crs is None:
            gdf = gdf.set_crs("EPSG:4326")
        self.priority_routes = gdf.to_crs(TARGET_CRS)
        print(f"  Loaded {len(self.priority_routes)} priority route features.")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _get_main_line(geom) -> LineString | None:
        """Return the longest LineString from a (Multi)LineString geometry."""
        if isinstance(geom, LineString):
            return geom
        if isinstance(geom, MultiLineString):
            return max(geom.geoms, key=lambda g: g.length)
        return None

    # ------------------------------------------------------------------
    # Metric calculation
    # ------------------------------------------------------------------

    def get_is_segment_on_srp(self, roadways) -> None:
        """Flag segments overlapping Critical or High SRP routes.

        Uses a 150-meter buffer around the road segment centroids and checks
        for intersection with SRP routes classified as Critical or High.

        Parameters
        ----------
        roadways:
            Object with a ``GA_RDWY_INV`` GeoDataFrame attribute.
        """
        print("Calculating SRP priority route flags ...")
        gdf = roadways.GA_RDWY_INV

        if self.priority_routes is None or self.priority_routes.empty:
            print("  No priority route data – setting all flags to False.")
            gdf[self.IS_SRP_CRITICAL_OR_HIGH] = False
            return

        # Filter to Critical and High only
        crit_high = self.priority_routes[
            self.priority_routes["priority_level"].isin(["Critical", "High"])
        ].copy()

        if crit_high.empty:
            gdf[self.IS_SRP_CRITICAL_OR_HIGH] = False
            return

        # Build a small buffer around each road segment for robust matching
        seg_buffers = gpd.GeoDataFrame(
            {"unique_id": gdf["unique_id"].values},
            geometry=gdf.geometry.buffer(150),  # 150 m tolerance
            crs=gdf.crs,
        )

        # Spatial join
        joined = gpd.sjoin(seg_buffers, crit_high, how="inner", predicate="intersects")
        matching_ids = joined["unique_id"].unique()

        gdf[self.IS_SRP_CRITICAL_OR_HIGH] = gdf["unique_id"].isin(matching_ids)
        print(
            f"  {gdf[self.IS_SRP_CRITICAL_OR_HIGH].sum()} segments flagged as "
            "Critical/High SRP."
        )

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    def clear_data(self) -> None:
        """Release loaded data to free memory."""
        self.priority_routes = None
        print("Priority route data cleared.")
