# FAF5 Route-Number Compatibility Check

## Context

Erik Martinez's Massachusetts Freight.py uses perpendicular-line spatial
conflation with route-number matching: FAF5 `Rte_Number` is matched against
MassDOT's `Route_Numb` field. This note assesses whether Georgia's staged
roadway inventory has equivalent fields for the same join.

## Available Route-Number Fields in Staged Output

| Field | Source | Format | Example |
|---|---|---|---|
| `ROUTE_NUMBER` | `ROUTE_ID[5:11]` via `parse_route_id()` | 6-char zero-padded string | `"000075"`, `"000041"` |
| `BASE_ROUTE_NUMBER` | `route_family.py::extract_base_route_number()` | Integer (trailing 3 or 6 digits depending on function type) | `75`, `41`, `316` |
| `HPMS_ROUTE_NUMBER` | HPMS 2024 `routenumber` field | Integer (cast from HPMS) | `75`, `41` |
| `ROUTE_TYPE_GDOT` | `route_type_gdot.py` | 1-3 char code | `I`, `US`, `SR`, `CR`, `CS`, `RP` |
| `HWY_NAME` | `route_type_gdot.py::_derive_hwy_name()` | Display string | `I-75`, `US-41`, `SR-316`, `RAMP` |

## FAF5 `Rte_Number` Format

FAF5 network links use a string route number like `I0075`, `U0041`, `S0316`.
The prefix encodes the route signing (I = Interstate, U = U.S. Route, S = State
Route) and the numeric portion is zero-padded to 4 digits.

## Compatibility Assessment

`BASE_ROUTE_NUMBER` is the best candidate for matching against FAF5 because:

1. It is an integer already stripped of GDOT-internal padding and function-type
   encoding. A simple `str(base_route_number).zfill(4)` reproduces the numeric
   portion of the FAF5 `Rte_Number`.

2. `ROUTE_TYPE_GDOT` maps directly to the FAF5 prefix convention:
   - `I` -> `I` prefix
   - `US` -> `U` prefix
   - `SR` -> `S` prefix

3. A composite key `f"{prefix}{base_route_number:04d}"` would reconstruct the
   FAF5 `Rte_Number` format for attribute-based pre-screening before the
   spatial perpendicular-line conflation step.

`HPMS_ROUTE_NUMBER` is a useful cross-check but has lower coverage than
`BASE_ROUTE_NUMBER` (only populated where HPMS matched the segment).

`ROUTE_NUMBER` (raw 6-char string) is less useful because it includes
GDOT-internal encoding for ramps and function types that does not correspond
to any FAF5 convention.

## Normalization Step Needed

A lightweight normalization function will be needed for Phase 2 FAF5 matching:

```python
FAF5_PREFIX_MAP = {"I": "I", "US": "U", "SR": "S"}

def to_faf5_rte_number(route_type_gdot: str, base_route_number: int) -> str | None:
    prefix = FAF5_PREFIX_MAP.get(route_type_gdot)
    if prefix is None or base_route_number is None:
        return None
    return f"{prefix}{base_route_number:04d}"
```

This is straightforward and does not require any additional data downloads.
County roads and local streets do not appear in FAF5 so the prefix map only
needs the three state-system families.
