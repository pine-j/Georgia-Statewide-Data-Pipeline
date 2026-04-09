"""Traffic generator proximity analysis for Georgia road segments.

Loads seven categories of traffic generators (airports, seaports,
universities, military bases, national parks, intermodal rail facilities,
and freight generators) and counts how many fall within a 5-mile buffer
of each road segment.

Georgia does not have international border crossings, so that category
is omitted compared to the Texas pipeline.
"""

from pathlib import Path

import geopandas as gpd
import pandas as pd

TARGET_CRS = "EPSG:32617"
BUFFER_MILES = 5
BUFFER_METERS = BUFFER_MILES * 1609.34


class TrafficGenerators:
    """Count traffic generators near each road segment."""

    TOTAL_TRAFFIC_GENERATORS = (
        "Proximity_to_Major_Traffic_Generators_(5_mi._buffer)"
    )
    GENERATORS_DENSITY = "Density_of_Major_Traffic_Generators"

    # Relative to project root
    _CLEAN_DIR = Path("02-Data-Staging") / "tables" / "connectivity"

    # Dataset name -> (filename stem, geometry type hint)
    _POINT_LAYERS = {
        "airport_count": "airports",
        "seaport_count": "seaports",
        "rail_facilities_count": "rail_facilities",
        "freight_gen_count": "freight_generators",
    }
    _POLYGON_LAYERS = {
        "university_count": "universities",
        "military_base_count": "military_bases",
        "national_park_count": "national_parks",
    }

    def __init__(self, project_root: Path | None = None):
        self._root = project_root or Path(__file__).resolve().parents[5]
        self._clean_dir = self._root / self._CLEAN_DIR

        # Loaded GeoDataFrames (populated by load_data)
        self.airports: gpd.GeoDataFrame | None = None
        self.seaports: gpd.GeoDataFrame | None = None
        self.universities: gpd.GeoDataFrame | None = None
        self.military_bases: gpd.GeoDataFrame | None = None
        self.national_parks: gpd.GeoDataFrame | None = None
        self.rail_facilities: gpd.GeoDataFrame | None = None
        self.freight_generators: gpd.GeoDataFrame | None = None

    # ------------------------------------------------------------------
    # Data loading
    # ------------------------------------------------------------------

    def load_data(self) -> None:
        """Read normalized GeoJSON files for all generator types."""
        print("Loading traffic generator data ...")
        for attr in (
            "airports", "seaports", "universities", "military_bases",
            "national_parks", "rail_facilities", "freight_generators",
        ):
            path = self._clean_dir / f"{attr}.geojson"
            if path.exists():
                gdf = gpd.read_file(path)
                if gdf.crs is None:
                    gdf = gdf.set_crs("EPSG:4326")
                gdf = gdf.to_crs(TARGET_CRS)
                setattr(self, attr, gdf)
                print(f"  Loaded {attr}: {len(gdf)} features")
            else:
                print(f"  WARNING: {path.name} not found – skipping.")
        print("Traffic generator data loaded.")

    # ------------------------------------------------------------------
    # Metric calculation
    # ------------------------------------------------------------------

    def get_traffic_generators_count(self, roadways) -> None:
        """Count generators within a 5-mile buffer of each road segment.

        Parameters
        ----------
        roadways:
            Object with a ``Roadway_Inventory`` GeoDataFrame attribute (the Georgia
            roadway inventory), which is updated in-place with new columns.
        """
        print("Calculating traffic generator counts ...")

        gdf = roadways.Roadway_Inventory

        # Build buffers
        buffers = gpd.GeoDataFrame(
            geometry=gdf.geometry.buffer(BUFFER_METERS),
            crs=gdf.crs,
        )
        buffers["unique_id"] = gdf["unique_id"].values
        buffers["buffer_area_sq_mile"] = buffers.geometry.area / (1609.34 ** 2)

        all_counts: list[pd.DataFrame] = []

        def _process(layer_name: str, generator_gdf, predicate: str):
            if generator_gdf is None or generator_gdf.empty:
                print(f"  Skipping {layer_name} (no data).")
                return None
            print(f"  Processing {layer_name} ...")
            gen_proj = generator_gdf.to_crs(TARGET_CRS)
            joined = gpd.sjoin(buffers, gen_proj, how="inner", predicate=predicate)
            counts = (
                joined.groupby("unique_id").size().reset_index(name=layer_name)
            )
            return counts

        # Point layers
        for col_name, attr_name in self._POINT_LAYERS.items():
            counts = _process(col_name, getattr(self, attr_name, None), "contains")
            if counts is not None:
                all_counts.append(counts)

        # Polygon layers
        for col_name, attr_name in self._POLYGON_LAYERS.items():
            counts = _process(col_name, getattr(self, attr_name, None), "intersects")
            if counts is not None:
                all_counts.append(counts)

        # Merge back
        result = gdf[["unique_id"]].copy()
        result = result.merge(
            buffers[["unique_id", "buffer_area_sq_mile"]],
            on="unique_id",
            how="left",
        )
        all_col_names = list(self._POINT_LAYERS) + list(self._POLYGON_LAYERS)
        for df in all_counts:
            result = result.merge(df, on="unique_id", how="left")

        for col in all_col_names:
            if col in result.columns:
                result[col] = result[col].fillna(0).astype(int)
            else:
                result[col] = 0

        result[self.TOTAL_TRAFFIC_GENERATORS] = result[all_col_names].sum(axis=1)
        result[self.GENERATORS_DENSITY] = (
            result[self.TOTAL_TRAFFIC_GENERATORS] / result["buffer_area_sq_mile"]
        ).round(2)

        # Merge into roadway inventory
        merge_cols = all_col_names + [
            self.TOTAL_TRAFFIC_GENERATORS,
            self.GENERATORS_DENSITY,
        ]
        existing = [c for c in merge_cols if c in roadways.Roadway_Inventory.columns]
        if existing:
            roadways.Roadway_Inventory.drop(columns=existing, inplace=True)

        roadways.Roadway_Inventory = roadways.Roadway_Inventory.merge(
            result[["unique_id"] + merge_cols],
            on="unique_id",
            how="left",
        )
        print("Traffic generator metrics complete.")

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    def clear_data(self) -> None:
        """Release loaded GeoDataFrames to free memory."""
        for attr in (
            "airports", "seaports", "universities", "military_bases",
            "national_parks", "rail_facilities", "freight_generators",
        ):
            setattr(self, attr, None)
        print("Traffic generator data cleared.")
