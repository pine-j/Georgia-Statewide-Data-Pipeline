-- Administrative boundary tables for the six new Georgia geographies.
-- Real load-bearing boundary data lives in the staged GeoPackage
-- (02-Data-Staging/spatial/base_network.gpkg); these tables exist so
-- the Postgres DDL stays symmetric with the staged layer set. Seeding
-- is left to the data-loading path that also ingests roadway_segments.
--
-- City boundaries are intentionally omitted - cities are a filter-only
-- geography in the webapp, not a map overlay. 535 polygons would
-- clutter the map with no planning value.

CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS area_office_boundaries (
    area_office_id INTEGER PRIMARY KEY,
    area_office_district INTEGER NOT NULL,
    area_office_area_id INTEGER NOT NULL,
    area_office_name TEXT NOT NULL,
    geometry geometry(MultiPolygon, 4326) NOT NULL
);

CREATE INDEX IF NOT EXISTS area_office_boundaries_geometry_idx
    ON area_office_boundaries
    USING GIST (geometry);

CREATE INDEX IF NOT EXISTS area_office_boundaries_district_idx
    ON area_office_boundaries (area_office_district);


CREATE TABLE IF NOT EXISTS mpo_boundaries (
    mpo_id TEXT PRIMARY KEY,
    mpo_name TEXT NOT NULL,
    geometry geometry(MultiPolygon, 4326) NOT NULL
);

CREATE INDEX IF NOT EXISTS mpo_boundaries_geometry_idx
    ON mpo_boundaries
    USING GIST (geometry);


CREATE TABLE IF NOT EXISTS regional_commission_boundaries (
    rc_id INTEGER PRIMARY KEY,
    rc_name TEXT NOT NULL,
    geometry geometry(MultiPolygon, 4326) NOT NULL
);

CREATE INDEX IF NOT EXISTS regional_commission_boundaries_geometry_idx
    ON regional_commission_boundaries
    USING GIST (geometry);


CREATE TABLE IF NOT EXISTS state_house_boundaries (
    state_house_district INTEGER PRIMARY KEY,
    state_house_name TEXT,
    geometry geometry(MultiPolygon, 4326) NOT NULL
);

CREATE INDEX IF NOT EXISTS state_house_boundaries_geometry_idx
    ON state_house_boundaries
    USING GIST (geometry);


CREATE TABLE IF NOT EXISTS state_senate_boundaries (
    state_senate_district INTEGER PRIMARY KEY,
    state_senate_name TEXT,
    geometry geometry(MultiPolygon, 4326) NOT NULL
);

CREATE INDEX IF NOT EXISTS state_senate_boundaries_geometry_idx
    ON state_senate_boundaries
    USING GIST (geometry);


CREATE TABLE IF NOT EXISTS congressional_boundaries (
    congressional_district INTEGER PRIMARY KEY,
    congressional_name TEXT,
    geometry geometry(MultiPolygon, 4326) NOT NULL
);

CREATE INDEX IF NOT EXISTS congressional_boundaries_geometry_idx
    ON congressional_boundaries
    USING GIST (geometry);
