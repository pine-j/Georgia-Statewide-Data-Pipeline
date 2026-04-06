from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.dependencies import get_db
from app.schemas import BoundsResponse
from app.services.geospatial import get_state_bounds


router = APIRouter(prefix="/geospatial", tags=["geospatial"])


@router.get("/bounds", response_model=BoundsResponse)
def get_bounds(
    state: str = Query(default="ga", min_length=2, max_length=8),
    district: int | None = Query(default=None, ge=1, le=7),
    county: list[str] | None = Query(default=None),
    db: Session = Depends(get_db),
) -> BoundsResponse:
    return BoundsResponse(
        bounds=get_state_bounds(
            db,
            state.lower(),
            district=district,
            counties=county,
        )
    )
