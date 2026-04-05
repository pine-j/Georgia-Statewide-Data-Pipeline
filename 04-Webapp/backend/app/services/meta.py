from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.settings import get_settings
from app.schemas import GeorgiaFilterOptionsResponse, StateOption
from app.services.georgia_filters import get_georgia_filter_options
from app.services.seed_data import list_seed_states


def list_states(db: Session | None) -> list[StateOption]:
    if get_settings().data_mode == "seed" or db is None:
        return list_seed_states()

    query = text(
        """
        SELECT code, name
        FROM app_states
        ORDER BY name;
        """
    )
    rows = db.execute(query).mappings().all()
    return [StateOption(code=row["code"], name=row["name"]) for row in rows]


def get_georgia_filters() -> GeorgiaFilterOptionsResponse:
    return get_georgia_filter_options()
