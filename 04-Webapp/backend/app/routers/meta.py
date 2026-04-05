from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.core.settings import Settings
from app.dependencies import get_app_settings, get_db
from app.schemas import AppConfigResponse, GeorgiaFilterOptionsResponse, StateOption
from app.services.meta import get_georgia_filters, list_states


router = APIRouter(prefix="/meta", tags=["meta"])


@router.get("/states", response_model=list[StateOption])
def get_states(db: Session = Depends(get_db)) -> list[StateOption]:
    return list_states(db)


@router.get("/config", response_model=AppConfigResponse)
def get_config(settings: Settings = Depends(get_app_settings)) -> AppConfigResponse:
    return AppConfigResponse(
        default_state=settings.default_state,
        map_provider=settings.map_provider,
        chat_enabled=False,
    )


@router.get("/georgia-filters", response_model=GeorgiaFilterOptionsResponse)
def get_georgia_filter_metadata() -> GeorgiaFilterOptionsResponse:
    return get_georgia_filters()
