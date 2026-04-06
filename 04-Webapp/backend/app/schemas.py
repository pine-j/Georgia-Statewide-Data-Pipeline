from typing import Any, Literal

from pydantic import BaseModel


class HealthResponse(BaseModel):
    status: str
    environment: str


class StateOption(BaseModel):
    code: str
    name: str


class DistrictOption(BaseModel):
    id: int
    label: str


class CountyOption(BaseModel):
    county: str
    county_fips: str
    district: int


class GeorgiaFilterOptionsResponse(BaseModel):
    districts: list[DistrictOption]
    counties: list[CountyOption]


class AppConfigResponse(BaseModel):
    default_state: str
    map_provider: str
    chat_enabled: bool


class FunctionalClassSummary(BaseModel):
    functional_class: str
    segment_count: int
    total_miles: float


class AnalyticsSummaryResponse(BaseModel):
    state_code: str
    roadway_count: int
    total_miles: float
    classes: list[FunctionalClassSummary]


class RoadwayFeatureProperties(BaseModel):
    id: int
    unique_id: str
    road_name: str
    functional_class: str
    aadt: int | None
    length_miles: float
    district: int
    district_label: str
    county: str


class RoadwayFeature(BaseModel):
    type: Literal["Feature"]
    geometry: dict[str, Any]
    properties: RoadwayFeatureProperties


class RoadwayFeatureCollection(BaseModel):
    type: Literal["FeatureCollection"]
    features: list[RoadwayFeature]


class GeoJsonFeature(BaseModel):
    type: Literal["Feature"]
    geometry: dict[str, Any]
    properties: dict[str, Any]


class GeoJsonFeatureCollection(BaseModel):
    type: Literal["FeatureCollection"]
    features: list[GeoJsonFeature]


class RoadwayDetailResponse(BaseModel):
    unique_id: str
    road_name: str
    district: int
    district_label: str
    county: str
    attributes: dict[str, Any]


class RoadwayManifestResponse(BaseModel):
    state_code: str
    total_segments: int
    chunk_size: int
    chunk_count: int
    bounds: list[float] | None


class BoundsResponse(BaseModel):
    bounds: list[float] | None
