from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.settings import get_settings
from app.schemas import AnalyticsSummaryResponse, FunctionalClassSummary, RoadwayFilters
from app.services.seed_data import get_seed_summary
from app.services.staged_roadways import get_staged_roadway_summary


def get_roadway_summary(
    db: Session | None,
    state_code: str,
    filters: RoadwayFilters | None = None,
) -> AnalyticsSummaryResponse:
    filters = filters or RoadwayFilters()
    data_mode = get_settings().data_mode

    if data_mode == "seed":
        return get_seed_summary(state_code, filters=filters)

    if data_mode == "staged":
        return get_staged_roadway_summary(state_code, filters=filters)

    if db is None:
        return AnalyticsSummaryResponse(
            state_code=state_code,
            roadway_count=0,
            total_miles=0.0,
            classes=[],
        )

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

    summary_query = text(
        f"""
        SELECT
            functional_class,
            COUNT(*) AS segment_count,
            COALESCE(SUM(length_miles), 0) AS total_miles
        FROM roadway_segments
        WHERE {' AND '.join(where_clauses)}
        GROUP BY functional_class
        ORDER BY functional_class;
        """
    )

    rows = db.execute(summary_query, params).mappings().all()

    classes = [
        FunctionalClassSummary(
            functional_class=row["functional_class"],
            segment_count=int(row["segment_count"]),
            total_miles=float(row["total_miles"]),
        )
        for row in rows
    ]

    roadway_count = sum(item.segment_count for item in classes)
    total_miles = round(sum(item.total_miles for item in classes), 2)

    return AnalyticsSummaryResponse(
        state_code=state_code,
        roadway_count=roadway_count,
        total_miles=total_miles,
        classes=classes,
    )
