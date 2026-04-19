from dataclasses import dataclass
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


class AreaOfficeOption(BaseModel):
    id: int
    label: str
    # The compound AREA_OFFICE_ID encodes parent_district (id // 100); this
    # field surfaces it explicitly for the UI cascade.
    parent_district: int


class MpoOption(BaseModel):
    id: str
    label: str


class RegionalCommissionOption(BaseModel):
    id: int
    label: str


class StateHouseOption(BaseModel):
    id: int
    label: str


class StateSenateOption(BaseModel):
    id: int
    label: str


class CongressionalOption(BaseModel):
    id: int
    label: str


class CityOption(BaseModel):
    id: int
    label: str
    # Populated via majority-by-length overlay at option-build time; nullable
    # for cities that straddle county/district boundaries in ambiguous ways.
    county: str | None = None
    district: int | None = None


class GeorgiaFilterOptionsResponse(BaseModel):
    districts: list[DistrictOption]
    counties: list[CountyOption]
    highway_types: list[HighwayTypeOption]
    area_offices: list[AreaOfficeOption] = Field(default_factory=list)
    mpos: list[MpoOption] = Field(default_factory=list)
    regional_commissions: list[RegionalCommissionOption] = Field(default_factory=list)
    state_house_districts: list[StateHouseOption] = Field(default_factory=list)
    state_senate_districts: list[StateSenateOption] = Field(default_factory=list)
    congressional_districts: list[CongressionalOption] = Field(default_factory=list)
    cities: list[CityOption] = Field(default_factory=list)


@dataclass(frozen=True)
class RoadwayFilters:
    """Immutable bundle of every segment filter the services thread through.

    Constructed once at the API edge (FastAPI query parameters -> this bundle)
    and passed by reference into every downstream query builder. Hashable by
    virtue of being a frozen dataclass over tuples, so lru_cache on the
    services that wrap SQL queries works without a separate cache_key().

    Empty defaults mean "no constraint" for that dimension. `cities` holds
    positive CITY_IDs to include; `include_unincorporated` is the explicit
    sentinel that maps to CITY_ID IS NULL in the WHERE builder (represents
    the "Unincorporated" pseudo-option at the top of the city autocomplete).
    """

    district: tuple[int, ...] = ()
    counties: tuple[str, ...] = ()
    highway_route_families: tuple[str, ...] = ()
    area_offices: tuple[int, ...] = ()
    mpos: tuple[str, ...] = ()
    regional_commissions: tuple[int, ...] = ()
    state_house_districts: tuple[int, ...] = ()
    state_senate_districts: tuple[int, ...] = ()
    congressional_districts: tuple[int, ...] = ()
    cities: tuple[int, ...] = ()
    include_unincorporated: bool = False

    def is_empty(self) -> bool:
        return (
            not self.district
            and not self.counties
            and not self.highway_route_families
            and not self.area_offices
            and not self.mpos
            and not self.regional_commissions
            and not self.state_house_districts
            and not self.state_senate_districts
            and not self.congressional_districts
            and not self.cities
            and not self.include_unincorporated
        )


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
    district_name: str
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
    # Step 2 geometry-authoritative admin geographies.
    area_office_id: int | None = None
    area_office_name: str | None = None
    mpo_id: str | None = None
    mpo_name: str | None = None
    rc_id: int | None = None
    rc_name: str | None = None
    # Step 4 overlay flags. city_id is the 48-bit name-hash; null = unincorporated.
    state_house_district: int | None = None
    state_senate_district: int | None = None
    congressional_district: int | None = None
    city_id: int | None = None
    city_name: str | None = None


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
    district_name: str
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


class ThemeFilterBin(BaseModel):
    value: str | None = None
    min_value: float | None = None
    max_value: float | None = None
    label: str
    default_selected: bool = True


class ThemeFilterSpec(BaseModel):
    control: Literal[
        "toggle_chips",
        "multi_select",
        "bin_multi_select",
        "range_slider",
        "hwy_des_matrix",
        "none",
    ]
    property_name: str
    bins: list[ThemeFilterBin] = Field(default_factory=list)
    min_bound: float | None = None
    max_bound: float | None = None
    step: float | None = None
    include_no_data_default: bool = True
    no_data_selectable: bool = True
    label: str | None = None
    description: str | None = None


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
    filters: list[ThemeFilterSpec] = Field(default_factory=list)


class RoadwayVisualizationCatalogResponse(BaseModel):
    default_option_id: str
    thematic_options: list[RoadwayVisualizationOption] = Field(default_factory=list)
    details_only_options: list[RoadwayVisualizationOption] = Field(default_factory=list)
    unavailable_options: list[RoadwayVisualizationOption] = Field(default_factory=list)
