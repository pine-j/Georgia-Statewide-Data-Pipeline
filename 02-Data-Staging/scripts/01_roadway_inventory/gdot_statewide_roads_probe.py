"""Probe and tile-extract the GDOT Statewide Roads ArcGIS REST layer.

This utility is intentionally exploratory. It is meant to answer questions like:

- does the live service respond reliably
- what schema does it expose
- can small AOIs be queried successfully
- can tiled extraction be resumed after transient failures

Outputs are written under:
    01-Raw-Data/GDOT_Statewide_Roads_Live/

Important limitation:
    Tiled extraction uses envelope intersection queries. Features that cross
    tile boundaries can therefore appear in more than one leaf tile. Treat the
    output as service-evaluation support, not as a production-ready merge input.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

LOGGER = logging.getLogger("gdot_statewide_roads_probe")

SERVICE_URL = "https://egisp.dot.ga.gov/arcgis/rest/services/ARCWEBSVCMAP/MapServer/4"
USER_AGENT = "Georgia-Statewide-Data-Pipeline exploratory probe"
PROJECT_ROOT = Path(__file__).resolve().parents[3]
OUTPUT_ROOT = PROJECT_ROOT / "01-Raw-Data" / "GDOT_Statewide_Roads_Live"
DEFAULT_RUN_NAME = "latest"
DEFAULT_FIELDS = [
    "RCLINK",
    "ROAD_NAME",
    "ROUTE_TYPE",
    "OWNERSHIP",
    "FUNCTIONAL_CLASS",
    "COUNTY_CODE",
    "GDOT_DISTRICT",
    "REVISION_DATE",
]


@dataclass(frozen=True)
class Envelope:
    """Bounding box in EPSG:3857."""

    xmin: float
    ymin: float
    xmax: float
    ymax: float
    wkid: int = 3857

    def as_esri_json(self) -> dict[str, Any]:
        return {
            "xmin": self.xmin,
            "ymin": self.ymin,
            "xmax": self.xmax,
            "ymax": self.ymax,
            "spatialReference": {"wkid": self.wkid},
        }

    def width(self) -> float:
        return self.xmax - self.xmin

    def height(self) -> float:
        return self.ymax - self.ymin

    def to_serializable(self) -> dict[str, Any]:
        return asdict(self)

    def subdivide(self) -> list["Envelope"]:
        mid_x = (self.xmin + self.xmax) / 2.0
        mid_y = (self.ymin + self.ymax) / 2.0
        return [
            Envelope(self.xmin, self.ymin, mid_x, mid_y, self.wkid),
            Envelope(mid_x, self.ymin, self.xmax, mid_y, self.wkid),
            Envelope(self.xmin, mid_y, mid_x, self.ymax, self.wkid),
            Envelope(mid_x, mid_y, self.xmax, self.ymax, self.wkid),
        ]


@dataclass(frozen=True)
class Tile:
    """A quadtree tile used during extraction."""

    tile_id: str
    envelope: Envelope
    depth: int

    def child_tiles(self) -> list["Tile"]:
        return [
            Tile(f"{self.tile_id}-{index}", child, self.depth + 1)
            for index, child in enumerate(self.envelope.subdivide())
        ]


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(message)s")


def mercator_from_wgs84(lon: float, lat: float) -> tuple[float, float]:
    lat = min(max(lat, -85.05112878), 85.05112878)
    x = lon * 20037508.34 / 180.0
    y = math.log(math.tan((90.0 + lat) * math.pi / 360.0)) / (math.pi / 180.0)
    y = y * 20037508.34 / 180.0
    return x, y


def envelope_from_args(
    bbox: list[float] | None,
    bbox_srid: int,
    metadata: dict[str, Any],
) -> Envelope:
    if bbox is None:
        extent = metadata.get("extent")
        if not extent:
            raise ValueError("Service metadata did not include an extent.")
        spatial_ref = extent.get("spatialReference", {})
        return Envelope(
            float(extent["xmin"]),
            float(extent["ymin"]),
            float(extent["xmax"]),
            float(extent["ymax"]),
            int(spatial_ref.get("latestWkid") or spatial_ref.get("wkid") or 3857),
        )

    xmin, ymin, xmax, ymax = bbox
    if bbox_srid == 4326:
        xmin, ymin = mercator_from_wgs84(xmin, ymin)
        xmax, ymax = mercator_from_wgs84(xmax, ymax)
        return Envelope(xmin, ymin, xmax, ymax, 3857)
    if bbox_srid == 3857:
        return Envelope(xmin, ymin, xmax, ymax, 3857)
    raise ValueError(f"Unsupported bbox SRID: {bbox_srid}")


class ArcGISRestClient:
    """Small ArcGIS REST client with retry/backoff."""

    def __init__(
        self,
        service_url: str,
        timeout_seconds: float,
        max_retries: int,
        backoff_seconds: float,
        session: requests.Session | None = None,
    ) -> None:
        self.service_url = service_url.rstrip("/")
        self.query_url = f"{self.service_url}/query"
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.backoff_seconds = backoff_seconds
        self.session = session or requests.Session()
        self.session.headers.setdefault("User-Agent", USER_AGENT)

    def _request_json(
        self,
        url: str,
        params: dict[str, Any],
        allow_geojson: bool = False,
    ) -> dict[str, Any]:
        last_error: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                LOGGER.debug("GET %s params=%s", url, params)
                response = self.session.get(url, params=params, timeout=self.timeout_seconds)
                response.raise_for_status()
                payload = response.json()
                if not allow_geojson and "error" in payload:
                    raise RuntimeError(f"ArcGIS error: {payload['error']}")
                if allow_geojson and payload.get("type") == "FeatureCollection":
                    return payload
                if "error" in payload:
                    raise RuntimeError(f"ArcGIS error: {payload['error']}")
                return payload
            except (requests.RequestException, ValueError, RuntimeError) as exc:
                last_error = exc
                if attempt >= self.max_retries:
                    break
                sleep_seconds = self.backoff_seconds * (2 ** (attempt - 1))
                LOGGER.warning(
                    "Request failed on attempt %s/%s: %s. Sleeping %.1fs before retry.",
                    attempt,
                    self.max_retries,
                    exc,
                    sleep_seconds,
                )
                time.sleep(sleep_seconds)

        raise RuntimeError(f"Request failed after {self.max_retries} attempts: {last_error}")

    def fetch_metadata(self) -> dict[str, Any]:
        return self._request_json(self.service_url, {"f": "pjson"})

    def query_count(self, envelope: Envelope, where: str = "1=1") -> int:
        payload = self._request_json(
            self.query_url,
            {
                "f": "json",
                "where": where,
                "returnCountOnly": "true",
                "geometry": json.dumps(envelope.as_esri_json(), separators=(",", ":")),
                "geometryType": "esriGeometryEnvelope",
                "inSR": envelope.wkid,
                "spatialRel": "esriSpatialRelIntersects",
            },
        )
        return int(payload["count"])

    def query_geojson(
        self,
        envelope: Envelope,
        fields: list[str],
        limit: int,
        where: str = "1=1",
    ) -> dict[str, Any]:
        return self._request_json(
            self.query_url,
            {
                "f": "geojson",
                "where": where,
                "outFields": ",".join(fields) if fields else "*",
                "returnGeometry": "true",
                "geometry": json.dumps(envelope.as_esri_json(), separators=(",", ":")),
                "geometryType": "esriGeometryEnvelope",
                "inSR": envelope.wkid,
                "spatialRel": "esriSpatialRelIntersects",
                "resultRecordCount": limit,
                "outSR": 4326,
            },
            allow_geojson=True,
        )

    def query_json_preview(
        self,
        envelope: Envelope,
        fields: list[str],
        limit: int,
        where: str = "1=1",
    ) -> dict[str, Any]:
        return self._request_json(
            self.query_url,
            {
                "f": "json",
                "where": where,
                "outFields": ",".join(fields) if fields else "*",
                "returnGeometry": "true",
                "geometry": json.dumps(envelope.as_esri_json(), separators=(",", ":")),
                "geometryType": "esriGeometryEnvelope",
                "inSR": envelope.wkid,
                "spatialRel": "esriSpatialRelIntersects",
                "resultRecordCount": limit,
                "outSR": 4326,
            },
        )


class CheckpointStore:
    """Persist extraction progress to a JSON checkpoint file."""

    def __init__(self, checkpoint_path: Path) -> None:
        self.checkpoint_path = checkpoint_path
        self.state: dict[str, Any] = {}

    def load_or_initialize(
        self,
        run_name: str,
        service_url: str,
        envelope: Envelope,
        args: argparse.Namespace,
    ) -> dict[str, Any]:
        if self.checkpoint_path.exists():
            self.state = json.loads(self.checkpoint_path.read_text(encoding="utf-8"))
            return self.state

        self.state = {
            "run_name": run_name,
            "service_url": service_url,
            "created_at": now_utc_iso(),
            "updated_at": now_utc_iso(),
            "root_envelope": envelope.to_serializable(),
            "settings": {
                "target_feature_count": args.target_feature_count,
                "max_depth": args.max_depth,
                "min_tile_width": args.min_tile_width,
                "min_tile_height": args.min_tile_height,
                "where": args.where,
                "fields": args.fields,
                "bbox_srid": args.bbox_srid,
            },
            "tiles": {},
            "summary": {
                "completed": 0,
                "empty": 0,
                "failed": 0,
                "subdivided": 0,
            },
        }
        self.save()
        return self.state

    def save(self) -> None:
        self.state["updated_at"] = now_utc_iso()
        self.checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
        self.checkpoint_path.write_text(json.dumps(self.state, indent=2), encoding="utf-8")

    def update_tile(self, tile: Tile, status: str, **details: Any) -> None:
        self.state["tiles"][tile.tile_id] = {
            "tile_id": tile.tile_id,
            "depth": tile.depth,
            "status": status,
            "envelope": tile.envelope.to_serializable(),
            **details,
        }
        summary = self.state["summary"]
        summary["completed"] = sum(
            1 for tile_data in self.state["tiles"].values() if tile_data["status"] == "completed"
        )
        summary["empty"] = sum(
            1 for tile_data in self.state["tiles"].values() if tile_data["status"] == "empty"
        )
        summary["failed"] = sum(
            1 for tile_data in self.state["tiles"].values() if tile_data["status"] == "failed"
        )
        summary["subdivided"] = sum(
            1 for tile_data in self.state["tiles"].values() if tile_data["status"] == "subdivided"
        )
        self.save()


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def fetch_and_store_metadata(client: ArcGISRestClient, output_dir: Path) -> dict[str, Any]:
    metadata = client.fetch_metadata()
    write_json(output_dir / "service_metadata.json", metadata)
    LOGGER.info("Saved service metadata to %s", output_dir / "service_metadata.json")
    return metadata


def ensure_fields(fields_arg: str | None) -> list[str]:
    if not fields_arg:
        return DEFAULT_FIELDS.copy()
    fields = [field.strip() for field in fields_arg.split(",") if field.strip()]
    return fields or DEFAULT_FIELDS.copy()


def tile_can_split(tile: Tile, min_tile_width: float, min_tile_height: float) -> bool:
    return tile.envelope.width() / 2.0 >= min_tile_width and tile.envelope.height() / 2.0 >= min_tile_height


def extract_tile(
    client: ArcGISRestClient,
    tile: Tile,
    args: argparse.Namespace,
    checkpoint: CheckpointStore,
    tiles_dir: Path,
    max_record_count: int,
) -> None:
    existing = checkpoint.state["tiles"].get(tile.tile_id)
    tile_output_path = tiles_dir / f"{tile.tile_id}.geojson"
    if existing and existing.get("status") == "completed" and tile_output_path.exists():
        LOGGER.info("Skipping completed tile %s", tile.tile_id)
        return

    LOGGER.info("Counting tile %s at depth %s", tile.tile_id, tile.depth)
    try:
        feature_count = client.query_count(tile.envelope, where=args.where)
    except Exception as exc:
        checkpoint.update_tile(tile, "failed", error=str(exc), stage="count")
        LOGGER.error("Tile %s count failed: %s", tile.tile_id, exc)
        return

    if feature_count == 0:
        checkpoint.update_tile(tile, "empty", feature_count=0)
        LOGGER.info("Tile %s is empty", tile.tile_id)
        return

    if (
        feature_count > args.target_feature_count
        and tile.depth < args.max_depth
        and tile_can_split(tile, args.min_tile_width, args.min_tile_height)
    ):
        checkpoint.update_tile(tile, "subdivided", feature_count=feature_count)
        LOGGER.info("Tile %s has %s features; subdividing.", tile.tile_id, feature_count)
        for child in tile.child_tiles():
            extract_tile(client, child, args, checkpoint, tiles_dir, max_record_count)
        return

    if feature_count > max_record_count:
        checkpoint.update_tile(
            tile,
            "failed",
            feature_count=feature_count,
            stage="extract",
            error=(
                f"Feature count {feature_count} exceeds layer maxRecordCount {max_record_count}. "
                "Increase max depth or lower target feature count."
            ),
        )
        LOGGER.error(
            "Tile %s has %s features, above maxRecordCount %s and cannot be safely fetched in one request.",
            tile.tile_id,
            feature_count,
            max_record_count,
        )
        return

    try:
        geojson_payload = client.query_geojson(
            tile.envelope,
            fields=ensure_fields(args.fields),
            limit=min(feature_count, max_record_count),
            where=args.where,
        )
        tile_output_path.parent.mkdir(parents=True, exist_ok=True)
        tile_output_path.write_text(json.dumps(geojson_payload, indent=2), encoding="utf-8")
        checkpoint.update_tile(
            tile,
            "completed",
            feature_count=feature_count,
            output=str(tile_output_path.relative_to(PROJECT_ROOT)),
        )
        LOGGER.info("Wrote tile %s to %s", tile.tile_id, tile_output_path)
    except Exception as exc:
        checkpoint.update_tile(
            tile,
            "failed",
            feature_count=feature_count,
            error=str(exc),
            stage="extract",
        )
        LOGGER.error("Tile %s extract failed: %s", tile.tile_id, exc)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Probe and tile-extract the GDOT ARCWEBSVCMAP Statewide Roads layer."
    )
    parser.add_argument("--service-url", default=SERVICE_URL, help="ArcGIS REST layer URL.")
    parser.add_argument(
        "--output-root",
        default=str(OUTPUT_ROOT),
        help="Root folder for saved metadata, probe results, and extract runs.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=45.0,
        help="Per-request timeout in seconds.",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=4,
        help="Maximum request attempts before failing.",
    )
    parser.add_argument(
        "--backoff-seconds",
        type=float,
        default=2.0,
        help="Base retry backoff in seconds.",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging.")

    subparsers = parser.add_subparsers(dest="command", required=True)

    metadata_parser = subparsers.add_parser("metadata", help="Fetch and save service metadata only.")
    metadata_parser.add_argument(
        "--run-name",
        default=DEFAULT_RUN_NAME,
        help="Output subdirectory name under the output root.",
    )

    probe_parser = subparsers.add_parser(
        "probe",
        help="Run a bounded preview or count query against the live service.",
    )
    probe_parser.add_argument(
        "--run-name",
        default=DEFAULT_RUN_NAME,
        help="Output subdirectory name under the output root.",
    )
    probe_parser.add_argument(
        "--bbox",
        nargs=4,
        type=float,
        metavar=("XMIN", "YMIN", "XMAX", "YMAX"),
        help="Bounding box for the probe query. Defaults to the service extent if omitted.",
    )
    probe_parser.add_argument(
        "--bbox-srid",
        type=int,
        choices=[4326, 3857],
        default=4326,
        help="SRID for --bbox. 4326 values are converted to Web Mercator for the service.",
    )
    probe_parser.add_argument("--where", default="1=1", help="ArcGIS SQL where clause.")
    probe_parser.add_argument(
        "--fields",
        default=",".join(DEFAULT_FIELDS),
        help="Comma-delimited output fields for non-count probes.",
    )
    probe_parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Maximum features to request for non-count probes.",
    )
    probe_parser.add_argument(
        "--count-only",
        action="store_true",
        help="Only request a count for the bbox instead of returning features.",
    )

    extract_parser = subparsers.add_parser(
        "extract",
        help="Recursively tile the service extent or a specified bbox and save leaf tiles.",
    )
    extract_parser.add_argument(
        "--run-name",
        default=DEFAULT_RUN_NAME,
        help="Output subdirectory name under the output root.",
    )
    extract_parser.add_argument(
        "--bbox",
        nargs=4,
        type=float,
        metavar=("XMIN", "YMIN", "XMAX", "YMAX"),
        help="Bounding box to extract. Defaults to the service extent if omitted.",
    )
    extract_parser.add_argument(
        "--bbox-srid",
        type=int,
        choices=[4326, 3857],
        default=4326,
        help="SRID for --bbox. 4326 values are converted to Web Mercator for the service.",
    )
    extract_parser.add_argument("--where", default="1=1", help="ArcGIS SQL where clause.")
    extract_parser.add_argument(
        "--fields",
        default=",".join(DEFAULT_FIELDS),
        help="Comma-delimited output fields for leaf-tile extraction.",
    )
    extract_parser.add_argument(
        "--target-feature-count",
        type=int,
        default=250,
        help="Subdivide tiles until a tile count is at or below this threshold.",
    )
    extract_parser.add_argument(
        "--max-depth",
        type=int,
        default=8,
        help="Maximum quadtree depth for subdivision.",
    )
    extract_parser.add_argument(
        "--min-tile-width",
        type=float,
        default=1000.0,
        help="Minimum tile width in EPSG:3857 meters before subdivision stops.",
    )
    extract_parser.add_argument(
        "--min-tile-height",
        type=float,
        default=1000.0,
        help="Minimum tile height in EPSG:3857 meters before subdivision stops.",
    )
    extract_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Write run scaffolding without issuing count or feature requests.",
    )

    return parser


def run_metadata(args: argparse.Namespace, client: ArcGISRestClient) -> int:
    output_dir = Path(args.output_root) / args.run_name / "metadata"
    fetch_and_store_metadata(client, output_dir)
    return 0


def run_probe(args: argparse.Namespace, client: ArcGISRestClient) -> int:
    output_dir = Path(args.output_root) / args.run_name / "probe"
    metadata = fetch_and_store_metadata(client, output_dir)
    envelope = envelope_from_args(args.bbox, args.bbox_srid, metadata)
    summary: dict[str, Any] = {
        "captured_at": now_utc_iso(),
        "service_url": args.service_url,
        "where": args.where,
        "bbox_3857": envelope.to_serializable(),
    }

    if args.count_only:
        summary["mode"] = "count_only"
        try:
            count = client.query_count(envelope, where=args.where)
            summary["status"] = "ok"
            summary["count"] = count
            LOGGER.info("Probe count: %s", count)
        except Exception as exc:
            summary["status"] = "failed"
            summary["error"] = str(exc)
            LOGGER.error("Probe count failed: %s", exc)
            write_json(output_dir / "probe_count.json", summary)
            return 1

        write_json(output_dir / "probe_count.json", summary)
        return 0

    summary["mode"] = "feature_preview"
    summary["limit"] = args.limit
    try:
        preview = client.query_json_preview(
            envelope,
            fields=ensure_fields(args.fields),
            limit=args.limit,
            where=args.where,
        )
        summary["status"] = "ok"
        summary["feature_count"] = len(preview.get("features", []))
        summary["response"] = preview
        LOGGER.info("Saved probe preview with %s features.", len(preview.get("features", [])))
        write_json(output_dir / "probe_preview.json", summary)
        return 0
    except Exception as exc:
        summary["status"] = "failed"
        summary["error"] = str(exc)
        write_json(output_dir / "probe_preview.json", summary)
        LOGGER.error("Probe preview failed: %s", exc)
        return 1


def run_extract(args: argparse.Namespace, client: ArcGISRestClient) -> int:
    run_dir = Path(args.output_root) / args.run_name / "extract"
    tiles_dir = run_dir / "tiles"
    metadata = fetch_and_store_metadata(client, run_dir)
    envelope = envelope_from_args(args.bbox, args.bbox_srid, metadata)

    max_record_count = int(metadata.get("maxRecordCount") or 1000)
    if args.target_feature_count > max_record_count:
        LOGGER.warning(
            "target-feature-count %s is above maxRecordCount %s; reducing target to match maxRecordCount.",
            args.target_feature_count,
            max_record_count,
        )
        args.target_feature_count = max_record_count

    write_json(
        run_dir / "run_config.json",
        {
            "captured_at": now_utc_iso(),
            "service_url": args.service_url,
            "bbox_3857": envelope.to_serializable(),
            "where": args.where,
            "fields": ensure_fields(args.fields),
            "target_feature_count": args.target_feature_count,
            "max_depth": args.max_depth,
            "min_tile_width": args.min_tile_width,
            "min_tile_height": args.min_tile_height,
            "dry_run": args.dry_run,
            "note": "Leaf tiles can contain duplicates where features cross tile boundaries.",
        },
    )

    checkpoint = CheckpointStore(run_dir / "checkpoint.json")
    checkpoint.load_or_initialize(args.run_name, args.service_url, envelope, args)
    root_tile = Tile("root", envelope, depth=0)

    if args.dry_run:
        checkpoint.update_tile(root_tile, "planned", note="Dry run only; no network tile queries issued.")
        LOGGER.info("Dry run complete. Wrote run scaffolding to %s", run_dir)
        return 0

    extract_tile(client, root_tile, args, checkpoint, tiles_dir, max_record_count)
    LOGGER.info("Extraction summary: %s", json.dumps(checkpoint.state["summary"], indent=2))
    return 0


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    configure_logging(args.verbose)

    client = ArcGISRestClient(
        service_url=args.service_url,
        timeout_seconds=args.timeout_seconds,
        max_retries=args.max_retries,
        backoff_seconds=args.backoff_seconds,
    )

    if args.command == "metadata":
        return run_metadata(args, client)
    if args.command == "probe":
        return run_probe(args, client)
    if args.command == "extract":
        return run_extract(args, client)

    parser.error(f"Unhandled command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
