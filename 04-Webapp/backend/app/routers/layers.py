from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.core.settings import get_settings
from app.dependencies import get_db
from app.schemas import (
    GeoJsonFeatureCollection,
    RoadwayDetailResponse,
    RoadwayFeatureCollection,
    RoadwayManifestResponse,
    RoadwayVisualizationCatalogResponse,
)
from app.services.layers import (
    get_boundary_features,
    get_roadway_detail,
    get_roadway_features,
    get_roadway_visualizations,
)
from app.services.staged_roadways import get_staged_roadway_manifest


router = APIRouter(prefix="/layers", tags=["layers"])


@router.get("/roadways/manifest", response_model=RoadwayManifestResponse)
def get_roadway_manifest(
    state: str = Query(default="ga", min_length=2, max_length=8),
    district: list[int] | None = Query(default=None),
    county: list[str] | None = Query(default=None),
    highway_type: list[str] | None = Query(default=None),
    chunk_size: int = Query(
        default=get_settings().staged_chunk_size,
        ge=1000,
        le=20000,
    ),
) -> RoadwayManifestResponse:
    return get_staged_roadway_manifest(
        state.lower(),
        chunk_size,
        district=district,
        counties=county,
        highway_types=highway_type,
    )


@router.get("/roadways", response_model=RoadwayFeatureCollection)
def get_roadways(
    state: str = Query(default="ga", min_length=2, max_length=8),
    district: list[int] | None = Query(default=None),
    county: list[str] | None = Query(default=None),
    highway_type: list[str] | None = Query(default=None),
    limit: int = Query(default=250, ge=1, le=20000),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
) -> RoadwayFeatureCollection:
    return get_roadway_features(
        db,
        state.lower(),
        limit,
        offset=offset,
        district=district,
        counties=county,
        highway_types=highway_type,
    )


@router.get("/roadways/visualizations", response_model=RoadwayVisualizationCatalogResponse)
def get_roadway_visualizations_endpoint() -> RoadwayVisualizationCatalogResponse:
    return get_roadway_visualizations()


@router.get("/roadways/detail", response_model=RoadwayDetailResponse)
def get_roadway_detail_endpoint(
    unique_id: str = Query(min_length=1),
    db: Session = Depends(get_db),
) -> RoadwayDetailResponse:
    detail = get_roadway_detail(db, unique_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="Roadway segment not found.")

    return detail


@router.get("/boundaries/{boundary_type}", response_model=GeoJsonFeatureCollection)
def get_boundaries(
    boundary_type: str,
    state: str = Query(default="ga", min_length=2, max_length=8),
    district: list[int] | None = Query(default=None),
    county: list[str] | None = Query(default=None),
) -> GeoJsonFeatureCollection:
    return get_boundary_features(
        state.lower(),
        boundary_type,
        district=district,
        counties=county,
    )
