CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS app_states (
    code TEXT PRIMARY KEY,
    name TEXT NOT NULL
);

INSERT INTO app_states (code, name)
VALUES ('ga', 'Georgia')
ON CONFLICT (code) DO NOTHING;

CREATE TABLE IF NOT EXISTS roadway_segments (
    id SERIAL PRIMARY KEY,
    state_code TEXT NOT NULL REFERENCES app_states(code),
    road_name TEXT NOT NULL,
    county_name TEXT NOT NULL,
    county_fips TEXT NOT NULL,
    district_id INTEGER NOT NULL,
    functional_class TEXT NOT NULL,
    aadt INTEGER,
    length_miles NUMERIC(10, 2) NOT NULL,
    geometry geometry(LineString, 4326) NOT NULL
);

CREATE INDEX IF NOT EXISTS roadway_segments_geometry_idx
    ON roadway_segments
    USING GIST (geometry);

INSERT INTO roadway_segments (state_code, road_name, county_name, county_fips, district_id, functional_class, aadt, length_miles, geometry)
VALUES
    (
        'ga',
        'GA 316 Corridor',
        'Clarke',
        '059',
        1,
        'Interstate',
        58200,
        5.42,
        ST_GeomFromText('LINESTRING(-83.5371 33.9401, -83.4078 33.9417)', 4326)
    ),
    (
        'ga',
        'I-20 Augusta',
        'Richmond',
        '245',
        2,
        'Principal Arterial',
        73400,
        6.18,
        ST_GeomFromText('LINESTRING(-82.1287 33.4769, -81.9737 33.5018)', 4326)
    ),
    (
        'ga',
        'I-75 Macon',
        'Bibb',
        '021',
        3,
        'Interstate',
        96800,
        7.36,
        ST_GeomFromText('LINESTRING(-83.6775 32.8818, -83.6020 32.7637)', 4326)
    ),
    (
        'ga',
        'US 82 Albany',
        'Dougherty',
        '095',
        4,
        'Minor Arterial',
        28400,
        4.91,
        ST_GeomFromText('LINESTRING(-84.1991 31.5808, -84.0715 31.5796)', 4326)
    ),
    (
        'ga',
        'I-16 Savannah',
        'Chatham',
        '051',
        5,
        'Interstate',
        81200,
        8.44,
        ST_GeomFromText('LINESTRING(-81.3094 32.1077, -81.0473 32.0468)', 4326)
    ),
    (
        'ga',
        'I-75 Cartersville',
        'Bartow',
        '015',
        6,
        'Interstate',
        101600,
        6.03,
        ST_GeomFromText('LINESTRING(-84.8521 34.1902, -84.7528 34.1487)', 4326)
    ),
    (
        'ga',
        'I-285 Eastside',
        'DeKalb',
        '089',
        7,
        'Interstate',
        167500,
        7.12,
        ST_GeomFromText('LINESTRING(-84.3018 33.9069, -84.2325 33.8388)', 4326)
    )
ON CONFLICT DO NOTHING;
