import json
from functools import lru_cache
from pathlib import Path

from app.core.settings import get_settings
from app.schemas import CountyOption, DistrictOption, GeorgiaFilterOptionsResponse
from app.services.staged_roadways import (
    get_staged_filter_options,
    list_highway_type_options,
)


DATA_PATH = Path(__file__).resolve().parent.parent / "data" / "georgia_filters.json"


@lru_cache(maxsize=1)
def get_georgia_filter_options() -> GeorgiaFilterOptionsResponse:
    if get_settings().data_mode == "staged":
        return get_staged_filter_options()

    payload = json.loads(DATA_PATH.read_text(encoding="utf-8"))
    return GeorgiaFilterOptionsResponse(
        districts=[DistrictOption(**item) for item in payload["districts"]],
        counties=[CountyOption(**item) for item in payload["counties"]],
        highway_types=list_highway_type_options(),
    )
