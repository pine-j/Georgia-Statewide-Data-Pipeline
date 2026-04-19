from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.settings import get_settings
from app.schemas import RoadwayFilters
from app.services.seed_data import get_seed_bounds
from app.services.staged_roadways import get_staged_roadway_bounds


def get_state_bounds(
    db: Session | None,
    state_code: str,
    filters: RoadwayFilters | None = None,
) -> list[float] | None:
    filters = filters or RoadwayFilters()
    data_mode = get_settings().data_mode

    if data_mode == "seed":
        return get_seed_bounds(state_code, filters=filters)

    if data_mode == "staged":
        return get_staged_roadway_bounds(state_code, filters=filters)

    if db is None:
        return None

    where_clauses = ["state_code = :state_code"]
    params: dict[str, object] = {"state_code": state_code}

    if filters.district:
        district_placeholders = []
        for index, district_id in enumerate(filters.district):
            param_name = f"district_{index}"
            district_placeholders.append(f":{param_name}")
            params[param_name] = district_id
        where_clauses.append(f"district_id IN ({', '.join(district_placeholders)})")

    if filters.counties:
        where_clauses.append("county_name = ANY(:counties)")
        params["counties"] = list(filters.counties)

    query = text(
        f"""
        WITH bounds AS (
            SELECT ST_Extent(geometry) AS extent
            FROM roadway_segments
            WHERE {' AND '.join(where_clauses)}
        )
        SELECT
            ST_XMin(extent) AS min_lng,
            ST_YMin(extent) AS min_lat,
            ST_XMax(extent) AS max_lng,
            ST_YMax(extent) AS max_lat
        FROM bounds
        WHERE extent IS NOT NULL;
        """
    )

    row = db.execute(query, params).mappings().first()
    if not row:
        return None

    return [
        float(row["min_lng"]),
        float(row["min_lat"]),
        float(row["max_lng"]),
        float(row["max_lat"]),
    ]
