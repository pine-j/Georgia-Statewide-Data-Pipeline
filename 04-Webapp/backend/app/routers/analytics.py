from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.dependencies import get_db
from app.schemas import AnalyticsSummaryResponse
from app.services.analytics import get_roadway_summary
from app.services.staged_roadways import resolve_filters_from_request


router = APIRouter(prefix="/analytics", tags=["analytics"])


@router.get("/summary", response_model=AnalyticsSummaryResponse)
def get_summary(
    state: str = Query(default="ga", min_length=2, max_length=8),
    district: list[int] | None = Query(default=None),
    county: list[str] | None = Query(default=None),
    highway_type: list[str] | None = Query(default=None),
    area_office: list[int] | None = Query(default=None),
    mpo: list[str] | None = Query(default=None),
    regional_commission: list[int] | None = Query(default=None),
    state_house: list[int] | None = Query(default=None),
    state_senate: list[int] | None = Query(default=None),
    congressional: list[int] | None = Query(default=None),
    city: list[int] | None = Query(default=None),
    include_unincorporated: bool = Query(default=False),
    db: Session = Depends(get_db),
) -> AnalyticsSummaryResponse:
    filters = resolve_filters_from_request(
        district=district,
        counties=county,
        highway_types=highway_type,
        area_offices=area_office,
        mpos=mpo,
        regional_commissions=regional_commission,
        state_house_districts=state_house,
        state_senate_districts=state_senate,
        congressional_districts=congressional,
        cities=city,
        include_unincorporated=include_unincorporated,
    )
    return get_roadway_summary(db, state.lower(), filters=filters)
