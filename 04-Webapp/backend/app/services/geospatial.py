from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.settings import get_settings
from app.services.seed_data import get_seed_bounds
from app.services.staged_roadways import get_staged_roadway_bounds


def get_state_bounds(
    db: Session | None,
    state_code: str,
    district: int | None = None,
    counties: list[str] | None = None,
) -> list[float] | None:
    data_mode = get_settings().data_mode

    if data_mode == "seed":
        return get_seed_bounds(state_code, district=district, counties=counties)

    if data_mode == "staged":
        return get_staged_roadway_bounds(
            state_code,
            district=district,
            counties=counties,
        )

    if db is None:
        return None

    where_clauses = ["state_code = :state_code"]
    params: dict[str, object] = {"state_code": state_code}

    if district is not None:
        where_clauses.append("district_id = :district")
        params["district"] = district

    if counties:
        where_clauses.append("county_name = ANY(:counties)")
        params["counties"] = counties

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
