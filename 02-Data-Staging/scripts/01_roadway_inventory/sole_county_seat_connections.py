"""Identify state routes that are the sole connection to a county seat.

A "sole county-seat connection" is a state route segment whose removal
would disconnect the county seat from the broader state route network.
These are cut edges (bridges) in the graph formed by the state route
system near each county seat.

Approach:
1. Load county seat locations (from Census or derived from county centroids).
2. Build a planar graph from state-system route segments (I, US, SR and
   their suffix variants) by snapping endpoints to a shared node grid.
3. For each county seat, find the nearest graph node and extract the
   local subgraph within a configurable radius.
4. Identify bridge edges (cut edges) in each county seat's subgraph.
5. Flag the corresponding roadway segments as IS_SOLE_COUNTY_SEAT_CONNECTION.

Libraries: networkx for graph analysis, shapely/geopandas for spatial ops.
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from pathlib import Path

import geopandas as gpd
import networkx as nx
import numpy as np
import pandas as pd
from shapely.geometry import LineString, MultiLineString, Point
from shapely.ops import nearest_points

LOGGER = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[3]

COUNTY_SEATS_PATH = (
    PROJECT_ROOT / "01-Raw-Data" / "connectivity" / "county_seats" / "ga_county_seats.geojson"
)

# Route types that form the state route system (not local roads).
STATE_SYSTEM_TYPES = frozenset({"I", "US", "SR", "SP", "BU", "CN", "BY", "LP", "AL", "BS", "CD", "FR"})

# Node snapping tolerance in CRS units (meters for EPSG:32617).
SNAP_TOLERANCE_M = 50.0

# Search radius around each county seat for subgraph extraction.
SEARCH_RADIUS_M = 30_000.0  # 30 km ≈ 18.6 miles

# Minimum subgraph size to attempt bridge detection.
MIN_SUBGRAPH_EDGES = 3


# Georgia county seats. Source: US Census Bureau, Georgia Secretary of State.
# Format: (county_name, seat_name, latitude, longitude)
GA_COUNTY_SEATS: list[tuple[str, str, float, float]] = [
    ("Appling", "Baxley", 31.7777, -82.3485),
    ("Atkinson", "Pearson", 31.2977, -82.8521),
    ("Bacon", "Alma", 31.5393, -82.4624),
    ("Baker", "Newton", 31.3106, -84.3330),
    ("Baldwin", "Milledgeville", 33.0801, -83.2321),
    ("Banks", "Homer", 34.3334, -83.4985),
    ("Barrow", "Winder", 33.9926, -83.7202),
    ("Bartow", "Cartersville", 34.1651, -84.7999),
    ("Ben Hill", "Fitzgerald", 31.7149, -83.2527),
    ("Berrien", "Nashville", 31.2074, -83.2502),
    ("Bibb", "Macon", 32.8407, -83.6324),
    ("Bleckley", "Cochran", 32.3868, -83.3546),
    ("Brantley", "Nahunta", 31.2049, -81.9810),
    ("Brooks", "Quitman", 30.7852, -83.5596),
    ("Bryan", "Pembroke", 32.1363, -81.6232),
    ("Bulloch", "Statesboro", 32.4488, -81.7832),
    ("Burke", "Waynesboro", 33.0901, -82.0157),
    ("Butts", "Jackson", 33.2943, -83.9660),
    ("Calhoun", "Morgan", 31.5382, -84.5988),
    ("Camden", "Woodbine", 30.9643, -81.7243),
    ("Candler", "Metter", 32.3960, -82.0602),
    ("Carroll", "Carrollton", 33.5801, -85.0766),
    ("Catoosa", "Ringgold", 34.9159, -85.1091),
    ("Charlton", "Folkston", 30.8316, -82.0085),
    ("Chatham", "Savannah", 32.0809, -81.0912),
    ("Chattahoochee", "Cusseta", 32.3054, -84.7749),
    ("Chattooga", "Summerville", 34.4731, -85.3477),
    ("Cherokee", "Canton", 34.2368, -84.4908),
    ("Clarke", "Athens", 33.9519, -83.3576),
    ("Clay", "Fort Gaines", 31.6082, -85.0457),
    ("Clayton", "Jonesboro", 33.5218, -84.3538),
    ("Clinch", "Homerville", 31.0363, -82.7499),
    ("Cobb", "Marietta", 33.9526, -84.5499),
    ("Coffee", "Douglas", 31.5088, -82.8499),
    ("Colquitt", "Moultrie", 31.1799, -83.7891),
    ("Columbia", "Appling", 33.5432, -82.3174),
    ("Cook", "Adel", 31.1374, -83.4235),
    ("Coweta", "Newnan", 33.3801, -84.7999),
    ("Crawford", "Knoxville", 32.7235, -83.9969),
    ("Crisp", "Cordele", 31.9635, -83.7741),
    ("Dade", "Trenton", 34.8726, -85.5091),
    ("Dawson", "Dawsonville", 34.4212, -84.1191),
    ("Decatur", "Bainbridge", 30.9035, -84.5755),
    ("DeKalb", "Decatur", 33.7748, -84.2963),
    ("Dodge", "Eastman", 32.1974, -83.1777),
    ("Dooly", "Vienna", 32.0918, -83.7955),
    ("Dougherty", "Albany", 31.5785, -84.1557),
    ("Douglas", "Douglasville", 33.7515, -84.7477),
    ("Early", "Blakely", 31.3774, -84.9346),
    ("Echols", "Statenville", 30.7016, -83.0271),
    ("Effingham", "Springfield", 32.3721, -81.3115),
    ("Elbert", "Elberton", 34.1107, -82.8688),
    ("Emanuel", "Swainsboro", 32.5974, -82.3343),
    ("Evans", "Claxton", 32.1618, -81.9040),
    ("Fannin", "Blue Ridge", 34.8626, -84.3238),
    ("Fayette", "Fayetteville", 33.4487, -84.4549),
    ("Floyd", "Rome", 34.2570, -85.1647),
    ("Forsyth", "Cumming", 34.2068, -84.1402),
    ("Franklin", "Carnesville", 34.3723, -83.2346),
    ("Fulton", "Atlanta", 33.7490, -84.3880),
    ("Gilmer", "Ellijay", 34.6948, -84.4822),
    ("Glascock", "Gibson", 33.2335, -82.5946),
    ("Glynn", "Brunswick", 31.1499, -81.4915),
    ("Gordon", "Calhoun", 34.5026, -84.9513),
    ("Grady", "Cairo", 30.8777, -84.2022),
    ("Greene", "Greensboro", 33.5790, -83.1824),
    ("Gwinnett", "Lawrenceville", 33.9562, -83.9880),
    ("Habersham", "Clarkesville", 34.6126, -83.5249),
    ("Hall", "Gainesville", 34.2979, -83.8241),
    ("Hancock", "Sparta", 33.2757, -82.9782),
    ("Haralson", "Buchanan", 33.8015, -85.1877),
    ("Harris", "Hamilton", 32.7576, -84.8749),
    ("Hart", "Hartwell", 34.3526, -82.9321),
    ("Heard", "Franklin", 33.2776, -85.0988),
    ("Henry", "McDonough", 33.4468, -84.1469),
    ("Houston", "Perry", 32.4585, -83.7316),
    ("Irwin", "Ocilla", 31.5938, -83.2527),
    ("Jackson", "Jefferson", 34.1126, -83.5985),
    ("Jasper", "Monticello", 33.3051, -83.6849),
    ("Jeff Davis", "Hazlehurst", 31.8688, -82.5968),
    ("Jefferson", "Louisville", 32.9868, -82.4118),
    ("Jenkins", "Millen", 32.8040, -81.9499),
    ("Johnson", "Wrightsville", 32.7268, -82.7199),
    ("Jones", "Gray", 33.0090, -83.5324),
    ("Lamar", "Barnesville", 33.0551, -84.1560),
    ("Lanier", "Lakeland", 31.0410, -83.0735),
    ("Laurens", "Dublin", 32.5407, -82.9038),
    ("Lee", "Leesburg", 31.7324, -84.1724),
    ("Liberty", "Hinesville", 31.8468, -81.5960),
    ("Lincoln", "Lincolnton", 33.7918, -82.4791),
    ("Long", "Ludowici", 31.7079, -81.7424),
    ("Lowndes", "Valdosta", 30.8327, -83.2785),
    ("Lumpkin", "Dahlonega", 34.5326, -83.9849),
    ("Macon", "Oglethorpe", 32.2943, -84.0635),
    ("Madison", "Danielsville", 34.1262, -83.2219),
    ("Marion", "Buena Vista", 32.3190, -84.5163),
    ("McDuffie", "Thomson", 33.4701, -82.5013),
    ("McIntosh", "Darien", 31.3710, -81.4338),
    ("Meriwether", "Greenville", 33.0290, -84.7127),
    ("Miller", "Colquitt", 31.1710, -84.7330),
    ("Mitchell", "Camilla", 31.2316, -84.2105),
    ("Monroe", "Forsyth", 33.0343, -83.9385),
    ("Montgomery", "Mount Vernon", 32.1785, -82.5957),
    ("Morgan", "Madison", 33.5951, -83.4688),
    ("Murray", "Chatsworth", 34.7659, -84.7699),
    ("Muscogee", "Columbus", 32.4610, -84.9877),
    ("Newton", "Covington", 33.5968, -83.8602),
    ("Oconee", "Watkinsville", 33.8626, -83.4024),
    ("Oglethorpe", "Lexington", 33.8690, -83.1132),
    ("Paulding", "Dallas", 33.9226, -84.8410),
    ("Peach", "Fort Valley", 32.5535, -83.8874),
    ("Pickens", "Jasper", 34.4676, -84.4291),
    ("Pierce", "Blackshear", 31.3060, -82.2424),
    ("Pike", "Zebulon", 33.1026, -84.3427),
    ("Polk", "Cedartown", 34.0137, -85.2555),
    ("Pulaski", "Hawkinsville", 32.2835, -83.4713),
    ("Putnam", "Eatonton", 33.3268, -83.3888),
    ("Quitman", "Georgetown", 31.8843, -85.0977),
    ("Rabun", "Clayton", 34.8776, -83.4010),
    ("Randolph", "Cuthbert", 31.7718, -84.7894),
    ("Richmond", "Augusta", 33.4735, -81.9749),
    ("Rockdale", "Conyers", 33.6676, -84.0177),
    ("Schley", "Ellaville", 32.2374, -84.3077),
    ("Screven", "Sylvania", 32.7518, -81.6360),
    ("Seminole", "Donalsonville", 31.0410, -84.8791),
    ("Spalding", "Griffin", 33.2468, -84.2641),
    ("Stephens", "Toccoa", 34.5776, -83.3324),
    ("Stewart", "Lumpkin", 32.0510, -84.7991),
    ("Sumter", "Americus", 32.0726, -84.2327),
    ("Talbot", "Talbotton", 32.6776, -84.5363),
    ("Taliaferro", "Crawfordville", 33.5540, -82.8946),
    ("Tattnall", "Reidsville", 32.0868, -82.1174),
    ("Taylor", "Butler", 32.5568, -84.2374),
    ("Telfair", "McRae-Helena", 31.9835, -82.8985),
    ("Terrell", "Dawson", 31.7735, -84.4474),
    ("Thomas", "Thomasville", 30.8366, -83.9786),
    ("Tift", "Tifton", 31.4499, -83.5085),
    ("Toombs", "Lyons", 32.2043, -82.3199),
    ("Towns", "Hiawassee", 34.9498, -83.7572),
    ("Treutlen", "Soperton", 32.3774, -82.5924),
    ("Troup", "LaGrange", 33.0390, -85.0313),
    ("Turner", "Ashburn", 31.7060, -83.6530),
    ("Twiggs", "Jeffersonville", 32.6868, -83.3463),
    ("Union", "Blairsville", 34.8759, -83.9558),
    ("Upson", "Thomaston", 32.8876, -84.3266),
    ("Walker", "LaFayette", 34.7098, -85.2822),
    ("Walton", "Monroe", 33.7940, -83.7132),
    ("Ware", "Waycross", 31.2135, -82.3560),
    ("Warren", "Warrenton", 33.4068, -82.6613),
    ("Washington", "Sandersville", 32.9818, -82.8110),
    ("Wayne", "Jesup", 31.5993, -81.8849),
    ("Webster", "Preston", 32.0618, -84.5374),
    ("Wheeler", "Alamo", 32.1474, -82.7785),
    ("White", "Cleveland", 34.5965, -83.7641),
    ("Whitfield", "Dalton", 34.7698, -84.9702),
    ("Wilcox", "Abbeville", 31.9921, -83.3069),
    ("Wilkes", "Washington", 33.7368, -82.7391),
    ("Wilkinson", "Irwinton", 32.8118, -83.1735),
    ("Worth", "Sylvester", 31.5310, -83.8360),
]


def _load_county_seats(crs: str | None = None) -> gpd.GeoDataFrame:
    """Load county seat point locations."""
    if COUNTY_SEATS_PATH.exists():
        seats = gpd.read_file(COUNTY_SEATS_PATH, engine="pyogrio")
        LOGGER.info("Loaded %d county seats from %s", len(seats), COUNTY_SEATS_PATH)
    else:
        seats = gpd.GeoDataFrame(
            [{"county": c, "seat_name": s, "lat": la, "lon": lo}
             for c, s, la, lo in GA_COUNTY_SEATS],
            geometry=[Point(lo, la) for _, _, la, lo in GA_COUNTY_SEATS],
            crs="EPSG:4326",
        )
        LOGGER.info("Using embedded county seat data: %d counties", len(seats))

    if crs and seats.crs and str(seats.crs) != str(crs):
        seats = seats.to_crs(crs)
    return seats


def _snap_coord(x: float, y: float, tolerance: float) -> tuple[float, float]:
    """Snap coordinates to a grid defined by tolerance."""
    return (
        round(x / tolerance) * tolerance,
        round(y / tolerance) * tolerance,
    )


def _build_route_graph(
    segments: gpd.GeoDataFrame,
    snap_tolerance: float = SNAP_TOLERANCE_M,
) -> tuple[nx.Graph, dict[int, list[tuple]]]:
    """Build a networkx graph from state-system route segments.

    Nodes are snapped endpoint coordinates. Edges carry the segment
    index and length.

    Returns (graph, seg_to_edges) where seg_to_edges maps segment
    index to the list of (node_a, node_b) edges it produced.
    """
    G = nx.Graph()
    seg_to_edges: dict[int, list[tuple]] = {}

    for idx in segments.index:
        geom = segments.at[idx, "geometry"]
        if geom is None or geom.is_empty:
            continue

        lines = []
        if isinstance(geom, LineString):
            lines = [geom]
        elif isinstance(geom, MultiLineString):
            lines = list(geom.geoms)
        else:
            continue

        edges = []
        for line in lines:
            coords = list(line.coords)
            if len(coords) < 2:
                continue

            start = _snap_coord(coords[0][0], coords[0][1], snap_tolerance)
            end = _snap_coord(coords[-1][0], coords[-1][1], snap_tolerance)

            if start == end:
                continue

            length = float(line.length)
            if G.has_edge(start, end):
                existing = G[start][end]
                existing.setdefault("segment_indices", []).append(idx)
            else:
                G.add_edge(start, end, length=length, segment_indices=[idx])

            edges.append((start, end))

        if edges:
            seg_to_edges[idx] = edges

    LOGGER.info(
        "Route graph: %d nodes, %d edges from %d segments",
        G.number_of_nodes(), G.number_of_edges(), len(seg_to_edges),
    )
    return G, seg_to_edges


def _find_nearest_node(
    graph: nx.Graph,
    point: Point,
) -> tuple | None:
    """Find the graph node nearest to the given point."""
    if graph.number_of_nodes() == 0:
        return None

    nodes = list(graph.nodes())
    min_dist = float("inf")
    nearest = None
    px, py = point.x, point.y

    for node in nodes:
        dist = (node[0] - px) ** 2 + (node[1] - py) ** 2
        if dist < min_dist:
            min_dist = dist
            nearest = node

    return nearest


def _extract_local_subgraph(
    graph: nx.Graph,
    center_node: tuple,
    radius: float,
) -> nx.Graph:
    """Extract a subgraph of all nodes within radius of center_node."""
    cx, cy = center_node
    nearby_nodes = [
        n for n in graph.nodes()
        if (n[0] - cx) ** 2 + (n[1] - cy) ** 2 <= radius ** 2
    ]
    return graph.subgraph(nearby_nodes).copy()


def apply_sole_county_seat_enrichment(
    gdf: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """Flag segments that are sole connections to county seats.

    Adds IS_SOLE_COUNTY_SEAT_CONNECTION (bool) and
    SOLE_CONNECTION_COUNTY_SEAT (county seat name).
    """
    enriched = gdf.copy()
    enriched["IS_SOLE_COUNTY_SEAT_CONNECTION"] = False
    enriched["SOLE_CONNECTION_COUNTY_SEAT"] = None

    seg_crs = enriched.crs
    if seg_crs is None:
        LOGGER.warning("Segments have no CRS — cannot analyze county seat connections")
        return enriched

    county_seats = _load_county_seats(crs=str(seg_crs))

    state_mask = pd.Series(True, index=enriched.index)
    if "ROUTE_TYPE_GDOT" in enriched.columns:
        state_mask = enriched["ROUTE_TYPE_GDOT"].isin(STATE_SYSTEM_TYPES)

    state_segments = enriched.loc[state_mask]
    if state_segments.empty:
        LOGGER.info("No state-system segments for county seat analysis")
        return enriched

    LOGGER.info(
        "Building route graph from %d state-system segments ...",
        len(state_segments),
    )
    graph, seg_to_edges = _build_route_graph(state_segments)

    if graph.number_of_edges() < MIN_SUBGRAPH_EDGES:
        LOGGER.warning("Route graph too small (%d edges)", graph.number_of_edges())
        return enriched

    total_bridges = 0
    seats_with_sole = []

    for seat_idx in county_seats.index:
        seat_point = county_seats.at[seat_idx, "geometry"]
        county = county_seats.at[seat_idx, "county"] if "county" in county_seats.columns else f"County_{seat_idx}"
        seat_name = county_seats.at[seat_idx, "seat_name"] if "seat_name" in county_seats.columns else county

        nearest_node = _find_nearest_node(graph, seat_point)
        if nearest_node is None:
            continue

        dist_to_node = ((nearest_node[0] - seat_point.x) ** 2 +
                        (nearest_node[1] - seat_point.y) ** 2) ** 0.5
        if dist_to_node > SEARCH_RADIUS_M:
            LOGGER.debug(
                "County seat %s is %.0f m from nearest node (> %.0f m threshold)",
                seat_name, dist_to_node, SEARCH_RADIUS_M,
            )
            continue

        subgraph = _extract_local_subgraph(graph, nearest_node, SEARCH_RADIUS_M)
        if subgraph.number_of_edges() < MIN_SUBGRAPH_EDGES:
            continue

        bridges = list(nx.bridges(subgraph))

        seat_bridges = []
        for u, v in bridges:
            edge_data = graph[u][v]
            seg_indices = edge_data.get("segment_indices", [])

            is_near_seat = (
                ((u[0] - nearest_node[0]) ** 2 + (u[1] - nearest_node[1]) ** 2) ** 0.5 < 5000
                or ((v[0] - nearest_node[0]) ** 2 + (v[1] - nearest_node[1]) ** 2) ** 0.5 < 5000
            )
            if is_near_seat and seg_indices:
                seat_bridges.append((u, v, seg_indices))

        if seat_bridges:
            seats_with_sole.append(seat_name)
            for u, v, seg_indices in seat_bridges:
                for seg_idx in seg_indices:
                    if seg_idx in enriched.index:
                        enriched.at[seg_idx, "IS_SOLE_COUNTY_SEAT_CONNECTION"] = True
                        existing = enriched.at[seg_idx, "SOLE_CONNECTION_COUNTY_SEAT"]
                        if existing and pd.notna(existing):
                            enriched.at[seg_idx, "SOLE_CONNECTION_COUNTY_SEAT"] = (
                                f"{existing}; {seat_name}"
                            )
                        else:
                            enriched.at[seg_idx, "SOLE_CONNECTION_COUNTY_SEAT"] = seat_name
                        total_bridges += 1

    unique_flagged = int(enriched["IS_SOLE_COUNTY_SEAT_CONNECTION"].sum())
    LOGGER.info(
        "Sole county-seat connections: %d segments flagged, "
        "%d county seats with sole connections: %s",
        unique_flagged, len(seats_with_sole),
        ", ".join(seats_with_sole[:10]) + ("..." if len(seats_with_sole) > 10 else ""),
    )

    return enriched
