from typing import Any, Literal

from pydantic import BaseModel, Field


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


class HighwayTypeOption(BaseModel):
    id: str
    label: str
    route_family: str


class GeorgiaFilterOptionsResponse(BaseModel):
    districts: list[DistrictOption]
    counties: list[CountyOption]
    highway_types: list[HighwayTypeOption]


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
    system_code_label: str | None = None
    direction_label: str | None = None
    num_lanes: int | None = None
    future_aadt_2044: int | None = None
    k_factor: int | None = None
    d_factor: int | None = None
    truck_aadt: int | None = None
    pct_sadt: float | None = None
    pct_cadt: float | None = None
    vmt: float | None = None
    nhs_ind_label: str | None = None
    median_type_label: str | None = None
    hwy_des: str | None = None
    speed_limit: int | None = None
    truck_pct: float | None = None
    functional_class_viz: str | None = None
    surface_type_label: str | None = None
    ownership_label: str | None = None
    facility_type_label: str | None = None
    sec_evac: str | None = None


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


class RoadwayLegendItem(BaseModel):
    color: str
    label: str
    value: str | None = None
    min_value: float | None = None
    max_value: float | None = None


class RoadwayVisualizationOption(BaseModel):
    id: str
    texas_header: str
    georgia_header: str | None = None
    label: str
    description: str
    notes: str | None = None
    kind: Literal["numeric", "categorical"] | None = None
    map_mode: Literal["thematic", "details_only", "unavailable"]
    implementation_status: Literal["staged", "derived", "popup_only", "unavailable"]
    property_name: str | None = None
    status: str
    unit: str | None = None
    default: bool = False
    no_data_color: str = "#b9c5ca"
    legend_items: list[RoadwayLegendItem] = Field(default_factory=list)


class RoadwayVisualizationCatalogResponse(BaseModel):
    default_option_id: str
    thematic_options: list[RoadwayVisualizationOption] = Field(default_factory=list)
    details_only_options: list[RoadwayVisualizationOption] = Field(default_factory=list)
    unavailable_options: list[RoadwayVisualizationOption] = Field(default_factory=list)
