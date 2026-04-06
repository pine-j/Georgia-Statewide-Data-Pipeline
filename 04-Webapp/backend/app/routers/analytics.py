from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.dependencies import get_db
from app.schemas import AnalyticsSummaryResponse
from app.services.analytics import get_roadway_summary


router = APIRouter(prefix="/analytics", tags=["analytics"])


@router.get("/summary", response_model=AnalyticsSummaryResponse)
def get_summary(
    state: str = Query(default="ga", min_length=2, max_length=8),
    district: int | None = Query(default=None, ge=1, le=7),
    county: list[str] | None = Query(default=None),
    db: Session = Depends(get_db),
) -> AnalyticsSummaryResponse:
    return get_roadway_summary(
        db,
        state.lower(),
        district=district,
        counties=county,
    )
