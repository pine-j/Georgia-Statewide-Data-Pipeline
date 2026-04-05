import json

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.settings import get_settings
from app.schemas import (
    GeoJsonFeatureCollection,
    RoadwayDetailResponse,
    RoadwayFeature,
    RoadwayFeatureCollection,
    RoadwayFeatureProperties,
)
from app.services.seed_data import get_seed_roadways
from app.services.staged_roadways import (
    get_district_label,
    get_staged_boundary_features,
    get_staged_roadway_detail,
    get_staged_roadway_features,
)


def get_roadway_features(
    db: Session | None,
    state_code: str,
    limit: int,
    offset: int = 0,
    district: int | None = None,
    counties: list[str] | None = None,
) -> RoadwayFeatureCollection:
    data_mode = get_settings().data_mode

    if data_mode == "seed":
        return get_seed_roadways(
            state_code,
            limit,
            district=district,
            counties=counties,
        )

    if data_mode == "staged":
        return get_staged_roadway_features(
            state_code,
            limit,
            offset=offset,
            district=district,
            counties=counties,
        )

    if db is None:
        return RoadwayFeatureCollection(type="FeatureCollection", features=[])

    where_clauses = ["state_code = :state_code"]
    params: dict[str, object] = {
        "state_code": state_code,
        "limit": limit,
        "offset": offset,
    }

    if district is not None:
        where_clauses.append("district_id = :district")
        params["district"] = district

    if counties:
        where_clauses.append("county_name = ANY(:counties)")
        params["counties"] = counties

    query = text(
        f"""
        SELECT
            id,
            road_name,
            functional_class,
            aadt,
            length_miles,
            district_id AS district,
            county_name AS county,
            ST_AsGeoJSON(geometry) AS geometry
        FROM roadway_segments
        WHERE {' AND '.join(where_clauses)}
        ORDER BY id
        LIMIT :limit OFFSET :offset;
        """
    )

    rows = db.execute(query, params).mappings().all()

    features = [
        RoadwayFeature(
            type="Feature",
            geometry=json.loads(row["geometry"]),
                properties=RoadwayFeatureProperties(
                    id=int(row["id"]),
                    unique_id=str(row["id"]),
                    road_name=row["road_name"],
                    functional_class=row["functional_class"],
                    aadt=row["aadt"],
                    length_miles=float(row["length_miles"]),
                    district=int(row["district"]),
                    district_label=get_district_label(int(row["district"])),
                    county=row["county"],
                ),
            )
            for row in rows
        ]

    return RoadwayFeatureCollection(type="FeatureCollection", features=features)


def get_roadway_detail(
    db: Session | None,
    unique_id: str,
) -> RoadwayDetailResponse | None:
    data_mode = get_settings().data_mode

    if data_mode == "staged":
        return get_staged_roadway_detail(unique_id)

    if data_mode == "seed" or db is None:
        return None

    return None


def get_boundary_features(
    state_code: str,
    boundary_type: str,
    district: int | None = None,
    counties: list[str] | None = None,
) -> GeoJsonFeatureCollection:
    data_mode = get_settings().data_mode

    if data_mode == "staged":
        return get_staged_boundary_features(
            state_code,
            boundary_type,
            district=district,
            counties=counties,
        )

    return GeoJsonFeatureCollection(type="FeatureCollection", features=[])
