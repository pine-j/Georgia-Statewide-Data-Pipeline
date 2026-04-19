"""Administrative boundary crossing detection for segmentation.

Step 2 of the re-segmentation plan: compute milepoints at which a route
geometry crosses any of the five split-driving administrative
geographies (County, GDOT District, Area Office, MPO, Regional
Commission), and stamp each cleanly-split segment with the
geometry-authoritative attributes of the polygon it falls inside.

City and legislative (State House / Senate / Congressional) layers
are NOT handled here - they are post-pass overlay flags in Step 4
(apply_admin_overlay_flags). Do not split geometry at city or
legislative lines.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass, field

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
from shapely.strtree import STRtree

logger = logging.getLogger(__name__)

# Must match normalize.MILEPOINT_TOLERANCE. Kept local to keep this module
# importable on its own.
MILEPOINT_TOLERANCE = 1e-4
MILEPOINT_PRECISION = 4


def _round_mp(value: float) -> float:
    rounded = round(float(value), MILEPOINT_PRECISION)
    if abs(rounded) < MILEPOINT_TOLERANCE:
        return 0.0
    return rounded


_POLYGONAL_TYPES = frozenset({"Polygon", "MultiPolygon"})


def _unwrap_polygonal(geom):
    """Yield polygonal geometries from a geom (Polygon/MultiPolygon pass
    through; GeometryCollection is walked recursively; LineString/Point
    parts are dropped with no error).
    """
    if geom is None or geom.is_empty:
        return
    gtype = geom.geom_type
    if gtype in _POLYGONAL_TYPES:
        yield geom
        return
    if gtype == "GeometryCollection":
        for sub in geom.geoms:
            yield from _unwrap_polygonal(sub)


@dataclass
class BoundaryCrosser:
    """One administrative geography whose boundaries drive segment splits.

    name: logical identifier for logging (e.g. 'county', 'mpo').
    gdf: polygon GeoDataFrame in the same projected CRS as the routes.
    attribute_cols: mapping of {source_column_in_gdf: output_column_name}.
        For each segment whose midpoint falls inside a polygon, every
        mapped source column's value is stamped under its output name.
        Example: {"COUNTYFP": "COUNTY_CODE", "NAME": "COUNTY_NAME"}.

    __post_init__ unwraps any GeometryCollection rows into their Polygon
    / MultiPolygon parts (retaining attributes) so a state-clipped MPO
    layer whose overlay returns mixed geometry types still covers
    correctly. Non-polygonal parts are dropped silently.
    """

    name: str
    gdf: gpd.GeoDataFrame
    attribute_cols: dict[str, str]
    _geoms: list = field(init=False, repr=False, default_factory=list)
    _tree: STRtree = field(init=False, repr=False, default=None)
    _boundaries: list = field(init=False, repr=False, default_factory=list)
    _boundary_tree: STRtree = field(init=False, repr=False, default=None)
    _row_index_map: list[int] = field(init=False, repr=False, default_factory=list)
    _boundary_index_map: list[int] = field(init=False, repr=False, default_factory=list)

    def __post_init__(self) -> None:
        # Unwrap GeometryCollection rows into atomic polygonal parts. Each
        # part inherits its source row's attributes. Non-polygonal parts
        # (LineString, Point) that may sneak through geopandas.overlay are
        # dropped; a WARNING is logged if any row contributed zero polygons.
        exploded_geoms: list = []
        exploded_row_idx: list[int] = []
        dropped_rows = 0
        dropped_non_polygonal = 0
        reset = self.gdf.reset_index(drop=True)
        for row_idx, geom in enumerate(reset.geometry):
            if geom is None or geom.is_empty:
                dropped_rows += 1
                continue
            if geom.geom_type not in _POLYGONAL_TYPES and geom.geom_type != "GeometryCollection":
                dropped_non_polygonal += 1
                continue
            initial_len = len(exploded_geoms)
            for poly in _unwrap_polygonal(geom):
                exploded_geoms.append(poly)
                exploded_row_idx.append(row_idx)
            if len(exploded_geoms) == initial_len and geom.geom_type == "GeometryCollection":
                # Collection contained no polygons (only lines/points) - drop.
                dropped_non_polygonal += 1

        if dropped_non_polygonal:
            logger.warning(
                "BoundaryCrosser(%s): dropped %d non-polygonal geometry rows",
                self.name,
                dropped_non_polygonal,
            )

        # Swap the GeoDataFrame for the exploded version so attrs_at_point
        # can look up attributes by compact index.
        if exploded_geoms:
            attr_cols = [c for c in reset.columns if c != reset.geometry.name]
            exploded_attrs = reset.iloc[exploded_row_idx][attr_cols].reset_index(drop=True)
            self.gdf = gpd.GeoDataFrame(exploded_attrs, geometry=exploded_geoms, crs=reset.crs)
        else:
            self.gdf = reset.iloc[0:0]

        self._geoms = list(self.gdf.geometry)
        self._row_index_map = list(range(len(self._geoms)))
        self._tree = STRtree(self._geoms) if self._geoms else None
        self._boundaries = [
            geom.boundary if geom is not None and not geom.is_empty else None
            for geom in self._geoms
        ]
        non_empty_boundaries = [
            (idx, b) for idx, b in enumerate(self._boundaries)
            if b is not None and not b.is_empty
        ]
        self._boundary_index_map = [idx for idx, _ in non_empty_boundaries]
        self._boundary_tree = (
            STRtree([b for _, b in non_empty_boundaries])
            if non_empty_boundaries
            else None
        )

    def polygon_index_containing(self, point: Point) -> int | None:
        """Return the gdf row index of the polygon covering `point`, else None."""
        if self._tree is None:
            return None
        for idx in self._tree.query(point):
            idx_int = int(idx)
            geom = self._geoms[idx_int]
            if geom is None or geom.is_empty:
                continue
            if geom.covers(point):
                return idx_int
        return None

    def attrs_at_point(self, point: Point) -> dict:
        """Return the attribute dict for the polygon covering `point`.

        Every output column is present; value is None when the point is
        outside every polygon (e.g. a rural segment outside any MPO).
        """
        out: dict = {out_col: None for out_col in self.attribute_cols.values()}
        idx = self.polygon_index_containing(point)
        if idx is None:
            return out
        row = self.gdf.iloc[idx]
        for src_col, out_col in self.attribute_cols.items():
            value = row.get(src_col) if src_col in row.index else None
            if value is None:
                continue
            try:
                is_na = pd.isna(value)
            except (TypeError, ValueError):
                is_na = False
            if is_na:
                continue
            out[out_col] = value
        return out


def _collect_crossing_milepoints(
    geom,
    milepoints: set,
    route_geom,
    route_start: float,
    route_span: float,
    line_length: float,
) -> None:
    """Walk an intersection geometry, projecting each Point back to a milepoint.

    Handles Point, MultiPoint, LineString, MultiLineString, and
    GeometryCollection. LineStrings (route ran along a boundary for some
    distance) contribute both endpoints as crossing milepoints.
    """
    if geom is None or geom.is_empty:
        return
    geom_type = geom.geom_type
    if geom_type == "Point":
        distance = route_geom.project(geom)
        milepoints.add(_round_mp(route_start + (distance / line_length) * route_span))
        return
    if geom_type == "MultiPoint":
        for sub in geom.geoms:
            distance = route_geom.project(sub)
            milepoints.add(_round_mp(route_start + (distance / line_length) * route_span))
        return
    if geom_type == "LineString":
        for coord in (geom.coords[0], geom.coords[-1]):
            p = Point(coord[0], coord[1])
            distance = route_geom.project(p)
            milepoints.add(_round_mp(route_start + (distance / line_length) * route_span))
        return
    if geom_type == "MultiLineString":
        for sub in geom.geoms:
            for coord in (sub.coords[0], sub.coords[-1]):
                p = Point(coord[0], coord[1])
                distance = route_geom.project(p)
                milepoints.add(_round_mp(route_start + (distance / line_length) * route_span))
        return
    if geom_type == "GeometryCollection":
        for sub in geom.geoms:
            _collect_crossing_milepoints(
                sub, milepoints, route_geom, route_start, route_span, line_length
            )
        return


def compute_route_crossings(
    route_geom,
    route_start_mp: float,
    route_end_mp: float,
    crossers: Sequence[BoundaryCrosser],
) -> list[float]:
    """Return sorted unique interior milepoints where `route_geom` crosses
    any polygon boundary in any crosser.

    Uses the same linear ratio math as slice_route_geometry:
        milepoint = route_start_mp + (distance_along / line_length) * route_span

    Endpoints (route_start_mp, route_end_mp) are NOT included; callers add
    them as breakpoints unconditionally.
    """
    if route_geom is None or route_geom.is_empty or not crossers:
        return []
    route_span = route_end_mp - route_start_mp
    line_length = route_geom.length
    if route_span <= MILEPOINT_TOLERANCE or line_length <= MILEPOINT_TOLERANCE:
        return []

    milepoints: set[float] = set()
    for crosser in crossers:
        if crosser._boundary_tree is None:
            continue
        candidate_idxs = crosser._boundary_tree.query(route_geom)
        for compact_idx in candidate_idxs:
            boundary = crosser._boundaries[
                crosser._boundary_index_map[int(compact_idx)]
            ]
            if boundary is None or boundary.is_empty:
                continue
            try:
                intersection = route_geom.intersection(boundary)
            except Exception as exc:  # noqa: BLE001 - defensive: skip bad geom
                logger.debug(
                    "intersection failed for crosser=%s: %s", crosser.name, exc
                )
                continue
            _collect_crossing_milepoints(
                intersection, milepoints, route_geom, route_start_mp, route_span, line_length
            )

    # Keep only interior milepoints (strictly between start and end).
    lower = route_start_mp + MILEPOINT_TOLERANCE
    upper = route_end_mp - MILEPOINT_TOLERANCE
    interior = [mp for mp in milepoints if lower <= mp <= upper]
    return sorted(interior)


def resolve_segment_admin_attrs(
    segment_piece,
    crossers: Sequence[BoundaryCrosser],
) -> dict:
    """Return the admin attribute dict for a cleanly-split segment.

    Samples the segment midpoint and queries each crosser's STRtree.
    Guaranteed single-valued per crosser because segments were split at
    every boundary crossing before this is called.
    """
    out: dict = {}
    if segment_piece is None or segment_piece.is_empty:
        for crosser in crossers:
            for out_col in crosser.attribute_cols.values():
                out.setdefault(out_col, None)
        return out
    midpoint = segment_piece.interpolate(0.5, normalized=True)
    for crosser in crossers:
        for out_col, value in crosser.attrs_at_point(midpoint).items():
            out[out_col] = value
    return out
