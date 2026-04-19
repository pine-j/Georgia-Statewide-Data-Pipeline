"""Synthetic tests for Step 6 backend changes.

Run:
    cd 04-Webapp/backend && python test_step6_backend.py

Exercises the pure-Python pieces: filter resolution, WHERE builders,
dispatch dict integrity. Does NOT touch the staged GPKG or any live
database - those verify at rollout.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from app.schemas import RoadwayFilters
from app.services import staged_roadways as svc


def test_resolve_filters_sorts_and_casts() -> None:
    f = svc.resolve_filters_from_request(
        district=[3, 1, 2],
        counties=["Fulton", "Dekalb"],  # case variations get resolved
        highway_types=["IH"],
        area_offices=[702, 104],
        mpos=["ATL", "  "],  # blank is dropped
        regional_commissions=[3, 1],
        state_house_districts=[42, 7],
        state_senate_districts=[1],
        congressional_districts=[14, 5],
        cities=[500, 100],
        include_unincorporated=True,
    )
    assert f.district == (1, 2, 3), f.district
    assert f.area_offices == (104, 702), f.area_offices
    assert f.regional_commissions == (1, 3)
    assert f.state_house_districts == (7, 42)
    assert f.congressional_districts == (5, 14)
    assert f.cities == (100, 500)
    assert "ATL" in f.mpos and "" not in f.mpos, f.mpos
    assert f.include_unincorporated is True


def test_roadway_filters_is_hashable_and_empty() -> None:
    """Frozen dataclass must be hashable (for lru_cache) and is_empty()
    must return True for the default instance."""
    empty = RoadwayFilters()
    assert empty.is_empty()
    hash(empty)  # must not raise
    with_filter = RoadwayFilters(district=(1,))
    assert not with_filter.is_empty()
    hash(with_filter)


def test_build_sqlite_filters_empty_returns_empty_where() -> None:
    where, params = svc._build_sqlite_filters(RoadwayFilters())
    assert where == "", where
    assert params == [], params


def test_build_sqlite_filters_wires_every_dim() -> None:
    f = RoadwayFilters(
        district=(1, 2),
        counties=("fulton",),
        highway_route_families=("Interstate",),
        area_offices=(702,),
        mpos=("ATL",),
        regional_commissions=(5,),
        state_house_districts=(42,),
        state_senate_districts=(7,),
        congressional_districts=(14,),
        cities=(100, 200),
        include_unincorporated=True,
    )
    where, params = svc._build_sqlite_filters(f)
    assert where.startswith("WHERE "), where
    for expected in (
        "DISTRICT IN",
        "AREA_OFFICE_ID IN",
        "MPO_ID IN",
        "RC_ID IN",
        "STATE_HOUSE_DISTRICT IN",
        "STATE_SENATE_DISTRICT IN",
        "CONGRESSIONAL_DISTRICT IN",
        "ROUTE_FAMILY IN",
        "CITY_ID IN",
        "CITY_ID IS NULL",
    ):
        assert expected in where, f"missing {expected!r} in {where}"
    assert 1 in params and 2 in params
    assert 100 in params and 200 in params
    assert "ATL" in params


def test_include_unincorporated_and_cities_combinations() -> None:
    """The four combinations of (cities, include_unincorporated) must
    produce the right WHERE fragment in BOTH builders (sqlite + gpkg).

    (a) cities=[42,17], incl=False  -> CITY_ID IN (42, 17)
    (b) cities=[],      incl=True   -> CITY_ID IS NULL
    (c) cities=[42],    incl=True   -> (CITY_ID IN (42) OR CITY_ID IS NULL)
    (d) cities=[],      incl=False  -> no city fragment at all
    """
    # --- (a) ---
    f = RoadwayFilters(cities=(42, 17), include_unincorporated=False)
    sql_where, sql_params = svc._build_sqlite_filters(f)
    gpkg_where = svc._build_gpkg_where(f)
    assert "CITY_ID IN (?, ?)" in sql_where, sql_where
    assert "CITY_ID IS NULL" not in sql_where, sql_where
    assert sql_params == [42, 17], sql_params
    assert "CITY_ID IN (42, 17)" in gpkg_where, gpkg_where
    assert "CITY_ID IS NULL" not in gpkg_where, gpkg_where

    # --- (b) ---
    f = RoadwayFilters(cities=(), include_unincorporated=True)
    sql_where, sql_params = svc._build_sqlite_filters(f)
    gpkg_where = svc._build_gpkg_where(f)
    assert "CITY_ID IS NULL" in sql_where, sql_where
    assert "CITY_ID IN" not in sql_where, sql_where
    assert sql_params == [], sql_params
    assert "CITY_ID IS NULL" in gpkg_where, gpkg_where
    assert "CITY_ID IN" not in gpkg_where, gpkg_where

    # --- (c) ---
    f = RoadwayFilters(cities=(42,), include_unincorporated=True)
    sql_where, sql_params = svc._build_sqlite_filters(f)
    gpkg_where = svc._build_gpkg_where(f)
    # Both fragments present, joined with OR, wrapped in parens
    assert "CITY_ID IN (?)" in sql_where, sql_where
    assert "CITY_ID IS NULL" in sql_where, sql_where
    assert " OR " in sql_where.split("(CITY_ID", 1)[1], sql_where
    assert sql_params == [42], sql_params
    assert "CITY_ID IN (42)" in gpkg_where, gpkg_where
    assert "CITY_ID IS NULL" in gpkg_where, gpkg_where
    assert " OR " in gpkg_where, gpkg_where

    # --- (d) ---
    f = RoadwayFilters(cities=(), include_unincorporated=False)
    sql_where, sql_params = svc._build_sqlite_filters(f)
    gpkg_where = svc._build_gpkg_where(f)
    assert "CITY_ID" not in sql_where, sql_where
    assert sql_params == [], sql_params
    assert "CITY_ID" not in gpkg_where, gpkg_where


def test_build_gpkg_where_quotes_mpo_and_city_not_quoted() -> None:
    f = RoadwayFilters(mpos=("ATL", "O'Malley"), cities=(100,))
    where = svc._build_gpkg_where(f)
    # MPO_ID values are quoted strings - embedded apostrophe should be escaped
    assert "MPO_ID IN ('ATL', 'O''Malley')" in where, where
    # CITY_ID values are int, not quoted
    assert "CITY_ID IN (100)" in where, where


def test_build_boundary_where_dispatches_by_layer() -> None:
    f = RoadwayFilters(district=(7,), area_offices=(702,))
    # area_office_boundaries: both district and area_offices apply
    where = svc._build_boundary_where("area_office_boundaries", f, ())
    assert "AREA_OFFICE_DISTRICT IN (7)" in where, where
    assert "AREA_OFFICE_ID IN (702)" in where, where
    # district_boundaries: only district applies
    where = svc._build_boundary_where("district_boundaries", f, ())
    assert "GDOT_DISTRICT IN (7)" in where, where
    assert "AREA_OFFICE_ID" not in where, where


def test_build_boundary_where_county_codes_go_to_county_layer() -> None:
    f = RoadwayFilters()
    where = svc._build_boundary_where("county_boundaries", f, ("001", "121"))
    assert "COUNTYFP IN ('001', '121')" in where, where


def test_build_boundary_where_unmapped_layer_returns_empty() -> None:
    f = RoadwayFilters(district=(1,))
    assert svc._build_boundary_where("city_boundaries", f, ()) == ""


def test_boundary_type_map_has_no_city() -> None:
    """City is intentionally filter-only; no map overlay for it."""
    assert "city" not in svc.BOUNDARY_TYPE_TO_LAYER
    assert "cities" not in svc.BOUNDARY_TYPE_TO_LAYER
    assert "city_boundaries" not in svc.BOUNDARY_FILTER_COLUMNS


def test_normalize_mpo_id_strips_trailing_float_zero() -> None:
    """MPO_ID arrives from SQLite as str(REAL) like '13197100.0'; the
    normalizer must collapse that to '13197100' so GPKG queries (which
    store MPO_ID as clean text) match, and so both float- and
    integer-stringified forms from the UI resolve to the same id.
    """
    assert svc._normalize_mpo_id("13197100.0") == "13197100"
    assert svc._normalize_mpo_id("13197100") == "13197100"
    assert svc._normalize_mpo_id(13197100.0) == "13197100"
    assert svc._normalize_mpo_id("  45199300.0  ") == "45199300"
    # Non-integer-looking strings are left alone (no over-stripping).
    assert svc._normalize_mpo_id("ATL") == "ATL"
    assert svc._normalize_mpo_id("1.5") == "1.5"
    # Blank / missing returns None.
    assert svc._normalize_mpo_id(None) is None
    assert svc._normalize_mpo_id("") is None
    assert svc._normalize_mpo_id("   ") is None

    # resolve_filters_from_request must apply normalization to both
    # float-stringified and clean-string forms from the UI.
    f = svc.resolve_filters_from_request(mpos=["13197100.0", "13197100"])
    assert f.mpos == ("13197100",), f.mpos


def test_dispatch_dict_uses_canonical_filter_attrs() -> None:
    """Every filter name in BOUNDARY_FILTER_COLUMNS must exist as a
    RoadwayFilters field (or be the special 'counties' name that's
    resolved via pre-computed county_codes in _build_boundary_where)."""
    filter_fields = set(RoadwayFilters.__dataclass_fields__.keys()) | {"counties"}
    for layer, spec in svc.BOUNDARY_FILTER_COLUMNS.items():
        for attr in spec:
            assert attr in filter_fields, f"{layer}: unknown filter attr {attr!r}"


TESTS = [
    test_resolve_filters_sorts_and_casts,
    test_roadway_filters_is_hashable_and_empty,
    test_build_sqlite_filters_empty_returns_empty_where,
    test_build_sqlite_filters_wires_every_dim,
    test_include_unincorporated_and_cities_combinations,
    test_build_gpkg_where_quotes_mpo_and_city_not_quoted,
    test_build_boundary_where_dispatches_by_layer,
    test_build_boundary_where_county_codes_go_to_county_layer,
    test_build_boundary_where_unmapped_layer_returns_empty,
    test_boundary_type_map_has_no_city,
    test_normalize_mpo_id_strips_trailing_float_zero,
    test_dispatch_dict_uses_canonical_filter_attrs,
]


def main() -> int:
    failures = 0
    for test in TESTS:
        try:
            test()
        except AssertionError as exc:
            print(f"FAIL {test.__name__}: {exc}")
            failures += 1
        except Exception as exc:  # noqa: BLE001
            print(f"ERROR {test.__name__}: {exc!r}")
            failures += 1
        else:
            print(f"ok   {test.__name__}")
    print(f"\n{len(TESTS) - failures}/{len(TESTS)} tests passed")
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
