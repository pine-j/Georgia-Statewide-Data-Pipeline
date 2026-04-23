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

`BASE_ROUTE_NUMBER` + `ROUTE_TYPE_GDOT` together can reconstruct the FAF5
`Rte_Number`, but with a critical caveat for Interstates:

**Interstate caveat**: `BASE_ROUTE_NUMBER` for Interstates contains GDOT
internal route numbers, NOT signed Interstate numbers. For example, I-75 has
`BASE_ROUTE_NUMBER = 401`, I-20 has `402`, I-85 has `403`. The mapping from
internal to signed numbers lives in `route_type_gdot.py` as
`INTERSTATE_ROUTE_NUMBER_TO_SIGNED` (line 22). The normalization function
MUST apply this mapping for Interstate rows, otherwise I-75 would produce
`I0401` instead of `I0075`.

For US and State Routes, `BASE_ROUTE_NUMBER` is the signed number directly
(e.g., US-41 = 41, SR-316 = 316). No extra mapping needed.

`ROUTE_TYPE_GDOT` maps to the FAF5 prefix convention:
- `I` -> `I` prefix (after signed-number lookup)
- `US` -> `U` prefix
- `SR` -> `S` prefix

**Suffix route types** (`BU`, `BS`, `BI`, `SP`, `CN`, `LP`, `AL`, `BY`)
share base numbers with their parent mainline routes but represent business
routes, spurs, connectors, loops, alternates, and bypasses. These are
intentionally excluded from FAF5 matching — FAF5 network links represent
mainline routes only. Suffix types that happen to spatially overlap with
FAF5 links will still be matched via the perpendicular-line spatial step.

`HPMS_ROUTE_NUMBER` is a useful cross-check but has lower coverage than
`BASE_ROUTE_NUMBER` (only populated where HPMS matched the segment). Note:
although described as integer, `hpms_enrichment.py` has a try/except that
falls back to string on cast failure.

`ROUTE_NUMBER` (raw 6-char string) is less useful because it includes
GDOT-internal encoding for ramps and function types that does not correspond
to any FAF5 convention.

## Normalization Step Needed

A lightweight normalization function will be needed for Phase 2 FAF5 matching:

```python
from route_type_gdot import INTERSTATE_ROUTE_NUMBER_TO_SIGNED

FAF5_PREFIX_MAP = {"I": "I", "US": "U", "SR": "S"}

def to_faf5_rte_number(route_type_gdot: str, base_route_number: int) -> str | None:
    prefix = FAF5_PREFIX_MAP.get(route_type_gdot)
    if prefix is None or base_route_number is None:
        return None
    if route_type_gdot == "I":
        signed = INTERSTATE_ROUTE_NUMBER_TO_SIGNED.get(base_route_number)
        if signed is None:
            return None
        return f"I{signed:04d}"
    return f"{prefix}{base_route_number:04d}"
```

County roads and local streets do not appear in FAF5 so the prefix map only
needs the three state-system families. Suffix route types (BU, SP, CN, etc.)
are excluded from attribute matching but may still match via spatial overlap.
