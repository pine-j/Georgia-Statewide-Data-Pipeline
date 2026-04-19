import json
from functools import lru_cache
from pathlib import Path
from typing import Any, Type

from app.core.settings import get_settings
from app.schemas import (
    AreaOfficeOption,
    CityOption,
    CongressionalOption,
    CountyOption,
    DistrictOption,
    GeorgiaFilterOptionsResponse,
    MpoOption,
    RegionalCommissionOption,
    StateHouseOption,
    StateSenateOption,
)
from app.services.staged_roadways import (
    get_staged_filter_options,
    list_highway_type_options,
)


DATA_PATH = Path(__file__).resolve().parent.parent / "data" / "georgia_filters.json"


def _load_section(
    payload: dict[str, Any], section: str, model_cls: Type[Any]
) -> list[Any]:
    """Tolerant section loader: missing/null sections degrade to an empty
    list. Lets us ship a georgia_filters.json that only seeds the minimum
    needed for local dev and lets the webapp's new filter categories
    simply render as empty instead of crashing.
    """
    items = payload.get(section) or []
    return [model_cls(**item) for item in items]


@lru_cache(maxsize=1)
def get_georgia_filter_options() -> GeorgiaFilterOptionsResponse:
    if get_settings().data_mode == "staged":
        return get_staged_filter_options()

    payload = json.loads(DATA_PATH.read_text(encoding="utf-8"))
    return GeorgiaFilterOptionsResponse(
        districts=_load_section(payload, "districts", DistrictOption),
        counties=_load_section(payload, "counties", CountyOption),
        highway_types=list_highway_type_options(),
        area_offices=_load_section(payload, "area_offices", AreaOfficeOption),
        mpos=_load_section(payload, "mpos", MpoOption),
        regional_commissions=_load_section(
            payload, "regional_commissions", RegionalCommissionOption
        ),
        state_house_districts=_load_section(
            payload, "state_house_districts", StateHouseOption
        ),
        state_senate_districts=_load_section(
            payload, "state_senate_districts", StateSenateOption
        ),
        congressional_districts=_load_section(
            payload, "congressional_districts", CongressionalOption
        ),
        cities=_load_section(payload, "cities", CityOption),
    )
