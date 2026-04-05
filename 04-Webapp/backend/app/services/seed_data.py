from app.schemas import (
    AnalyticsSummaryResponse,
    FunctionalClassSummary,
    RoadwayFeature,
    RoadwayFeatureCollection,
    RoadwayFeatureProperties,
    StateOption,
)


SEED_STATES = [
    StateOption(code="ga", name="Georgia"),
]

SEED_DISTRICT_LABELS = {
    1: "District 1 - Gainesville",
    2: "District 2 - Tennille",
    3: "District 3 - Thomaston",
    4: "District 4 - Tifton",
    5: "District 5 - Jesup",
    6: "District 6 - Cartersville",
    7: "District 7 - Chamblee",
}

SEED_ROADWAYS = [
    {
        "id": 1,
        "unique_id": "seed-1",
        "state_code": "ga",
        "road_name": "GA 316 Corridor",
        "county": "Clarke",
        "district": 1,
        "functional_class": "Interstate",
        "aadt": 58200,
        "length_miles": 5.42,
        "geometry": {
            "type": "LineString",
            "coordinates": [
                [-83.5371, 33.9401],
                [-83.4078, 33.9417],
            ],
        },
    },
    {
        "id": 2,
        "unique_id": "seed-2",
        "state_code": "ga",
        "road_name": "I-20 Augusta",
        "county": "Richmond",
        "district": 2,
        "functional_class": "Principal Arterial",
        "aadt": 73400,
        "length_miles": 6.18,
        "geometry": {
            "type": "LineString",
            "coordinates": [
                [-82.1287, 33.4769],
                [-81.9737, 33.5018],
            ],
        },
    },
    {
        "id": 3,
        "unique_id": "seed-3",
        "state_code": "ga",
        "road_name": "I-75 Macon",
        "county": "Bibb",
        "district": 3,
        "functional_class": "Interstate",
        "aadt": 96800,
        "length_miles": 7.36,
        "geometry": {
            "type": "LineString",
            "coordinates": [
                [-83.6775, 32.8818],
                [-83.6020, 32.7637],
            ],
        },
    },
    {
        "id": 4,
        "unique_id": "seed-4",
        "state_code": "ga",
        "road_name": "US 82 Albany",
        "county": "Dougherty",
        "district": 4,
        "functional_class": "Minor Arterial",
        "aadt": 28400,
        "length_miles": 4.91,
        "geometry": {
            "type": "LineString",
            "coordinates": [
                [-84.1991, 31.5808],
                [-84.0715, 31.5796],
            ],
        },
    },
    {
        "id": 5,
        "unique_id": "seed-5",
        "state_code": "ga",
        "road_name": "I-16 Savannah",
        "county": "Chatham",
        "district": 5,
        "functional_class": "Interstate",
        "aadt": 81200,
        "length_miles": 8.44,
        "geometry": {
            "type": "LineString",
            "coordinates": [
                [-81.3094, 32.1077],
                [-81.0473, 32.0468],
            ],
        },
    },
    {
        "id": 6,
        "unique_id": "seed-6",
        "state_code": "ga",
        "road_name": "I-75 Cartersville",
        "county": "Bartow",
        "district": 6,
        "functional_class": "Interstate",
        "aadt": 101600,
        "length_miles": 6.03,
        "geometry": {
            "type": "LineString",
            "coordinates": [
                [-84.8521, 34.1902],
                [-84.7528, 34.1487],
            ],
        },
    },
    {
        "id": 7,
        "unique_id": "seed-7",
        "state_code": "ga",
        "road_name": "I-285 Eastside",
        "county": "DeKalb",
        "district": 7,
        "functional_class": "Interstate",
        "aadt": 167500,
        "length_miles": 7.12,
        "geometry": {
            "type": "LineString",
            "coordinates": [
                [-84.3018, 33.9069],
                [-84.2325, 33.8388],
            ],
        },
    },
]


def list_seed_states() -> list[StateOption]:
    return SEED_STATES


def filter_seed_roadways(
    state_code: str,
    district: int | None = None,
    counties: list[str] | None = None,
) -> list[dict]:
    roadways = [item for item in SEED_ROADWAYS if item["state_code"] == state_code]

    if district is not None:
        roadways = [item for item in roadways if item["district"] == district]

    if counties:
        county_set = {county.lower() for county in counties}
        roadways = [item for item in roadways if item["county"].lower() in county_set]

    return roadways


def get_seed_summary(
    state_code: str,
    district: int | None = None,
    counties: list[str] | None = None,
) -> AnalyticsSummaryResponse:
    roadways = filter_seed_roadways(state_code, district=district, counties=counties)
    grouped: dict[str, list[dict]] = {}

    for roadway in roadways:
        grouped.setdefault(roadway["functional_class"], []).append(roadway)

    classes = [
        FunctionalClassSummary(
            functional_class=functional_class,
            segment_count=len(items),
            total_miles=round(sum(float(item["length_miles"]) for item in items), 2),
        )
        for functional_class, items in sorted(grouped.items())
    ]

    return AnalyticsSummaryResponse(
        state_code=state_code,
        roadway_count=len(roadways),
        total_miles=round(sum(float(item["length_miles"]) for item in roadways), 2),
        classes=classes,
    )


def get_seed_roadways(
    state_code: str,
    limit: int,
    district: int | None = None,
    counties: list[str] | None = None,
) -> RoadwayFeatureCollection:
    roadways = filter_seed_roadways(
        state_code,
        district=district,
        counties=counties,
    )[:limit]

    return RoadwayFeatureCollection(
        type="FeatureCollection",
        features=[
            RoadwayFeature(
                type="Feature",
                geometry=item["geometry"],
                properties=RoadwayFeatureProperties(
                    id=int(item["id"]),
                    unique_id=item["unique_id"],
                    road_name=item["road_name"],
                    functional_class=item["functional_class"],
                    aadt=item["aadt"],
                    length_miles=float(item["length_miles"]),
                    district=int(item["district"]),
                    district_label=SEED_DISTRICT_LABELS[int(item["district"])],
                    county=item["county"],
                ),
            )
            for item in roadways
        ],
    )


def get_seed_bounds(
    state_code: str,
    district: int | None = None,
    counties: list[str] | None = None,
) -> list[float] | None:
    roadways = filter_seed_roadways(state_code, district=district, counties=counties)
    if not roadways:
        return None

    coordinates = [
        coordinate
        for item in roadways
        for coordinate in item["geometry"]["coordinates"]
    ]
    lngs = [coordinate[0] for coordinate in coordinates]
    lats = [coordinate[1] for coordinate in coordinates]

    return [min(lngs), min(lats), max(lngs), max(lats)]
