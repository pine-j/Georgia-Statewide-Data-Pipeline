"""Connectivity category scoring for Georgia road segments.

Computes the following metrics per segment and rolls them into a weighted
Connectivity Needs Score:

    - Degrees_of_Connection_Count: count of unique intersecting highway
      names within a 1-mile buffer.
    - Total_Traffic_Generators: count of major generators within a 5-mile
      buffer (airports, seaports, universities, military bases, national
      parks, intermodal rail, freight generators).
    - Is_SRP_Critical_or_High: boolean flag from GDOT SRP priority data.

Follows the same pattern as the Texas Connectivity class in the upstream
RAPTOR_Pipeline repository.
"""

from pathlib import Path

import geopandas as gpd
import numpy as np

from .TrafficGenerators import TrafficGenerators
from .PriorityRoutes import PriorityRoutes


class Connectivity:
    """Georgia connectivity metrics and scoring."""

    # Column name constants
    DEGREES_OF_CONNECTION_COUNT = "Number_of_Connections_(1_mi._buffer)"

    CONNECTIVITY_SCORE = "Connectivity_Needs_Score"

    def __init__(self, project_root: Path | None = None):
        self._root = project_root
        self.traffic_generators = TrafficGenerators(project_root=project_root)
        self.priority_routes = PriorityRoutes(project_root=project_root)

        self.weights = {
            self.DEGREES_OF_CONNECTION_COUNT: 0.25,
            self.traffic_generators.TOTAL_TRAFFIC_GENERATORS: 0.25,
            self.priority_routes.IS_SRP_CRITICAL_OR_HIGH: 0.50,
        }

    # ------------------------------------------------------------------
    # Degrees of connection
    # ------------------------------------------------------------------

    def _calculate_degrees_of_connection(self, roadways) -> None:
        """Count unique intersecting highway names within a 1-mile buffer.

        For each road segment a 1-mile (1609.34 m) buffer is created, then a
        spatial join identifies all other segments whose geometry intersects
        that buffer.  Only *unique* highway names (excluding the segment's own
        highway) are counted.

        Parameters
        ----------
        roadways:
            Object with a ``Roadway_Inventory`` GeoDataFrame attribute that must
            contain ``unique_id``, ``geometry``, and ``HWY_NAME`` columns.
        """
        print("Calculating degrees of connection (1-mile buffer) ...")
        gdf = roadways.Roadway_Inventory

        if "HWY_NAME" not in gdf.columns:
            print("  WARNING: 'HWY_NAME' column not found â€“ skipping.")
            gdf[self.DEGREES_OF_CONNECTION_COUNT] = 0
            return

        # Build buffered version
        buffered = gdf[["unique_id", "geometry", "HWY_NAME"]].copy()
        buffered["geometry"] = buffered.geometry.buffer(1609.34)

        target = gdf[["unique_id", "geometry", "HWY_NAME"]]

        joined = gpd.sjoin(
            buffered,
            target,
            how="inner",
            predicate="intersects",
            lsuffix="left",
            rsuffix="right",
        )

        # Exclude self-matches (same highway name)
        joined = joined[joined["HWY_NAME_left"] != joined["HWY_NAME_right"]]

        # Count unique highway names per segment
        counts = (
            joined
            .groupby("unique_id_left")["HWY_NAME_right"]
            .nunique()
            .reset_index(name=self.DEGREES_OF_CONNECTION_COUNT)
        )
        counts.rename(columns={"unique_id_left": "unique_id"}, inplace=True)

        # Merge back
        if self.DEGREES_OF_CONNECTION_COUNT in gdf.columns:
            gdf.drop(columns=[self.DEGREES_OF_CONNECTION_COUNT], inplace=True)

        gdf = gdf.merge(counts, on="unique_id", how="left")
        gdf[self.DEGREES_OF_CONNECTION_COUNT] = (
            gdf[self.DEGREES_OF_CONNECTION_COUNT].fillna(0).astype(int)
        )

        roadways.Roadway_Inventory = gdf
        print(
            f"  Degrees of connection calculated. (Rows: {len(gdf)})"
        )

    # ------------------------------------------------------------------
    # Scoring
    # ------------------------------------------------------------------

    def _calculate_connectivity_score(self, roadways) -> None:
        """Compute the weighted Connectivity Needs Score.

        Numerical columns are min-max normalised to [0, 1] before weighting.
        Boolean columns contribute their weight directly (1.0 if True, 0.0 if
        False).
        """
        print("Computing Connectivity Needs Score ...")
        gdf = roadways.Roadway_Inventory
        score = np.zeros(len(gdf), dtype=float)

        for col, weight in self.weights.items():
            if col not in gdf.columns:
                print(f"  WARNING: '{col}' missing â€“ skipped in score.")
                continue

            series = gdf[col]
            if series.dtype == bool or set(series.dropna().unique()).issubset({0, 1, True, False}):
                score += series.astype(float) * weight
            else:
                col_min = series.min()
                col_max = series.max()
                if col_max > col_min:
                    normalised = (series - col_min) / (col_max - col_min)
                else:
                    normalised = 0.0
                score += normalised * weight

        gdf[self.CONNECTIVITY_SCORE] = score.round(4)
        roadways.Roadway_Inventory = gdf
        print("  Connectivity score calculated.")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate_metrics(self, roadways) -> None:
        """Run the full connectivity analysis pipeline.

        Parameters
        ----------
        roadways:
            Object with a ``Roadway_Inventory`` GeoDataFrame attribute.
        """
        # Degrees of connection
        self._calculate_degrees_of_connection(roadways)

        # Traffic generators
        self.traffic_generators.load_data()
        self.traffic_generators.get_traffic_generators_count(roadways)

        # SRP priority routes
        self.priority_routes.load_data()
        self.priority_routes.get_is_segment_on_srp(roadways)

        # Final score
        self._calculate_connectivity_score(roadways)

        # Cleanup
        self.traffic_generators.clear_data()
        self.priority_routes.clear_data()

        print("Connectivity metrics complete.")
