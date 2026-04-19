# Admin boundary downloader

Caches the 8 administrative boundary layers the Georgia RAPTOR roadway
inventory pipeline reads, so `normalize.py` can run without internet
once the cache is populated.

## Usage

```bash
# From the repo root, one-shot populate:
python 01-Raw-Data/Boundaries/scripts/download_boundaries.py

# Force re-download (refresh vintage):
python 01-Raw-Data/Boundaries/scripts/download_boundaries.py --force

# Only a subset:
python 01-Raw-Data/Boundaries/scripts/download_boundaries.py --only counties,mpos

# List registered sources:
python 01-Raw-Data/Boundaries/scripts/download_boundaries.py --list
```

## Layout

- `01-Raw-Data/Boundaries/scripts/` — the downloader (tracked in git)
- `01-Raw-Data/Boundaries/cache/` — downloaded artifacts (git-ignored)
  - `counties.fgb`, `districts.fgb`, `mpos.fgb`,
    `regional_commissions.fgb`, `cities.fgb` — FlatGeobuf, read
    directly by `normalize.py` via `pyogrio`
  - `state_house.zip` + `state_house/` extracted shapefile — raw
    TIGER/Line zip (matches what `normalize.py` would otherwise
    download) plus an unpacked copy for any tool that prefers .shp
  - `state_senate.zip` + `state_senate/`
  - `congressional.zip` + `congressional/`
  - `manifest.json` — timestamps, URLs, feature counts, and
    SHA-256 checksums per layer

## How `normalize.py` consumes the cache

`fetch_and_cache_boundary(..., local_cache_filename=...)` resolves each
filename relative to `01-Raw-Data/Boundaries/cache/`. If the file
exists, the live URL is skipped entirely. If it doesn't, the URL is
fetched (the existing behaviour). No code-path branching is needed at
call sites; passing a cache filename is the only difference.

## Re-run cadence

Sources republish on varying schedules:

- GDOT counties + districts — rare (years)
- FHWA MPO — quarterly-ish
- DCA Regional Commissions — rare (last 2021)
- ARC Cities — quarterly-ish
- Census TIGER/Line — annual (year bump = `TIGER_YEAR` constant
  in both this script and `normalize.py`)

Re-run the downloader with `--force` whenever you want a fresh vintage;
the `manifest.json` records what was downloaded when.

## Intentional omissions

**GDOT Layers 2, 4, 5, 6** are NOT included because they are stale
(Area Offices 2014 / legislative layers pre-2020-census). Area Office
polygons are derived in `normalize.py` from the County layer dissolved
by `02-Data-Staging/config/area_office_codes.json`. No cache file
exists for them.

**Cities** is cached here but not served as a map overlay in the
webapp — cities are filter-only. The file still needs to be present
because `normalize.py` uses it for both Fulton Area Office sub-polygon
derivation and the `CITY_ID` / `CITY_NAME` overlay flag.
