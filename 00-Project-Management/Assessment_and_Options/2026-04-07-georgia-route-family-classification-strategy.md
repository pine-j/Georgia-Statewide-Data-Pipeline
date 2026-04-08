# Georgia Route-Family Classification Strategy

## Purpose

Define a Georgia-specific, GDOT-grounded route-family crosswalk for Phase 1 and the roadway ETL.

This strategy adds a coarse statewide family field:

- `Interstate`
- `U.S. Route`
- `State Route`
- `Local/Other`

It also adds a more descriptive detail field for Georgia-specific subtypes such as:

- `Business`
- `Spur`
- `Bypass`
- `Connector`
- `Loop`
- `Ramp`
- `Collector Distributor`
- `Ramp-CD Connector`
- `Frontage Road`
- `County Road`
- `City Street`
- `Private Road`
- `Federal Route`

## Official Georgia Sources

- GDOT Road & Traffic Data: <https://www.dot.ga.gov/GDOT/Pages/RoadTrafficData.aspx>
- GDOT Understanding Route IDs: <https://www.dot.ga.gov/DriveSmart/Data/Documents/Guides/UnderstandingRouteIDs_Doc.pdf>
- GDOT Road Inventory Data Dictionary: <https://www.dot.ga.gov/DriveSmart/Data/Documents/Road_Inventory_Data_Dictionary.pdf>
- GDOT live LRS metadata: <https://rnhp.dot.ga.gov/hosting/rest/services/GDOT_Network_LRSN/MapServer/exts/LRSServer/layers>

## Georgia-Specific Findings

- GDOT says the road inventory covers more than 125,000 centerline miles of public roads and includes state routes, county roads, and city streets.
- GDOT's route-ID guide defines the 16-character `ROUTE_ID` as `FUNCTION_TYPE + COUNTY + SYSTEM_CODE + ROUTE_CODE + SUFFIX + DIRECTION`.
- GDOT's live LRS metadata documents `SYSTEM_CODE` values `1 = State Highway Routes`, `2 = Public Roads`, `3 = Private`, and `4 = Federal`.
- GDOT's route guide and data dictionary show that `FUNCTION_TYPE` is the authoritative source for operational subtypes such as `Ramp`, `Collector Distributor`, `Ramp-CD Connector`, and `Frontage Road`.
- GDOT Appendix F maps Interstate designations to state route codes.
- GDOT Appendix G maps U.S. Highway designations to state route codes.
- Georgia route IDs encode state route numbers, so Interstate and U.S. Highway identification is a crosswalk problem, not a simple prefix problem.

## Proposed Taxonomy

| Field | Purpose | Values |
|---|---|---|
| `ROUTE_FAMILY` | Broad signed or administrative family for filtering and reporting | `Interstate`, `U.S. Route`, `State Route`, `Local/Other` |
| `ROUTE_FAMILY_DETAIL` | Georgia-specific readable subtype | `Interstate Ramp`, `U.S. Route Business`, `County Road`, `City Street`, `Public Road Frontage Road`, etc. |
| `ROUTE_FAMILY_CONFIDENCE` | QA signal for downstream use | `high`, `medium` |
| `ROUTE_FAMILY_SOURCE` | Traceable rule source | `gdot_appendix_f_interstate`, `gdot_appendix_g_us_route`, `public_road_numeric_suffix`, etc. |
| `BASE_ROUTE_NUMBER` | Family-level route number used in crosswalk logic | numeric |
| `ROUTE_SUFFIX_LABEL` | Decoded alpha suffix where GDOT defines one | `Business`, `Spur`, `Connector`, `Loop`, `Express Lane`, etc. |

## What Can Be Derived From `ROUTE_ID` Alone

High-confidence from `ROUTE_ID` string structure:

- function type
- county code
- system code
- route code
- suffix
- direction
- county road vs city street within `SYSTEM_CODE = 2` public-road mainline records

High-confidence from `ROUTE_ID` plus official GDOT appendices:

- Interstate family using Appendix F route-code crosswalk

Medium-confidence from `ROUTE_ID` plus official GDOT appendices:

- U.S. Route family using Appendix G route-code crosswalk
- State Route family as the residual `SYSTEM_CODE = 1` bucket after Interstate and U.S. Route tests

Reason for medium confidence:

- Georgia stores state route numbers in `ROUTE_ID`, including hidden or concurrent route numbers used by U.S. Highways.
- A route code appearing in Appendix G means the code is used for a U.S. Highway, but it does not guarantee the roadway is exclusively or visibly signed as a U.S. route everywhere.

## What Requires Other Fields

Use `FUNCTION_TYPE` or the first `ROUTE_ID` digit for:

- `Ramp`
- `Collector Distributor`
- `Ramp-CD Connector`
- `Frontage Road`
- `Separate Managed Facility`
- `Alley`
- `Y-Connector`
- `Roundabout`

Use `F_SYSTEM` or `FUNCTIONAL_CLASS` for:

- Interstate vs arterial vs collector vs local functional hierarchy
- planning-grade network role

Use `NHS` and `STRAHNET` for:

- national-significance and defense-network context

Do not use `NHS` to infer signed route family.

## Crosswalk Rule Set

### 1. Parse the GDOT route structure

From the 16-character `ROUTE_ID`:

- char 1: `FUNCTION_TYPE`
- chars 2-4: county code
- char 5: `SYSTEM_CODE`
- chars 6-11: route code
- chars 12-13: suffix
- chars 14-16: direction

### 2. Derive `BASE_ROUTE_NUMBER`

- For normal mainline-style records, `BASE_ROUTE_NUMBER = int(route_code)`.
- For `FUNCTION_TYPE` `2`, `3`, or `4`, use the last three digits of the six-character route code because GDOT says digits 6-8 are reference post and digits 9-11 are the underlying route number.

### 3. Apply family priority

Use this exact order:

1. `Interstate`
2. `U.S. Route`
3. `State Route`
4. `Local/Other`

This prevents Interstate-coded segments that also appear in the U.S.-route appendix from being downgraded to `U.S. Route`.

### 4. Interstate rule

- If `SYSTEM_CODE = 1` and `BASE_ROUTE_NUMBER` is in the GDOT Interstate crosswalk, classify as `Interstate`.

Interstate route numbers from GDOT Appendix F:

- `401`, `402`, `403`, `404`, `405`, `406`, `407`, `408`, `409`, `411`, `413`, `415`, `417`, `419`, `421`

### 5. U.S. Route rule

- If `SYSTEM_CODE = 1`, not already `Interstate`, and `BASE_ROUTE_NUMBER` is in the GDOT Appendix G U.S.-route list, classify as `U.S. Route`.

Recommended confidence:

- `medium`

### 6. State Route rule

- If `SYSTEM_CODE = 1` and the route does not match the Interstate or U.S.-route crosswalks, classify as `State Route`.

Recommended confidence:

- `medium`

### 7. Local and other rules

- If `SYSTEM_CODE = 2` and mainline public-road suffix is `00`, classify `ROUTE_FAMILY = Local/Other`, detail `County Road`.
- If `SYSTEM_CODE = 2` and mainline public-road suffix is numeric `01`-`99`, classify `ROUTE_FAMILY = Local/Other`, detail `City Street`.
- If `SYSTEM_CODE = 2` and `FUNCTION_TYPE` is non-mainline, keep `ROUTE_FAMILY = Local/Other` and use detail from `FUNCTION_TYPE`.
- If `SYSTEM_CODE = 3`, classify `Local/Other`, detail `Private Road`.
- If `SYSTEM_CODE = 4`, classify `Local/Other`, detail `Federal Route`.

## Mapping Table

| Pattern or field rule | Output | Confidence | Notes |
|---|---|---|---|
| `SYSTEM_CODE = 1` and `BASE_ROUTE_NUMBER` in Appendix F Interstate set | `ROUTE_FAMILY = Interstate` | high | Official GDOT Interstate route-code crosswalk |
| `SYSTEM_CODE = 1` and not Interstate and `BASE_ROUTE_NUMBER` in Appendix G U.S. set | `ROUTE_FAMILY = U.S. Route` | medium | Official GDOT U.S. route-code crosswalk, but still state-route encoded |
| `SYSTEM_CODE = 1` and not Appendix F or G match | `ROUTE_FAMILY = State Route` | medium | Residual state-highway bucket |
| `SYSTEM_CODE = 2` and `FUNCTION_TYPE = 1` and suffix `00` | `ROUTE_FAMILY_DETAIL = County Road` | high | GDOT county-road example |
| `SYSTEM_CODE = 2` and `FUNCTION_TYPE = 1` and suffix `01`-`99` | `ROUTE_FAMILY_DETAIL = City Street` | high | GDOT city-street example |
| `FUNCTION_TYPE = 2` | `... Ramp` | high | Use as subtype, not as family |
| `FUNCTION_TYPE = 3` | `... Collector Distributor` | high | Use as subtype, not as family |
| `FUNCTION_TYPE = 4` | `... Ramp-CD Connector` | high | Use as subtype, not as family |
| `FUNCTION_TYPE = 5` | `... Frontage Road` | high | Use as subtype, not as family |
| suffix `BU` | `... Business` | high | Official GDOT suffix table |
| suffix `SP` | `... Spur` | high | Official GDOT suffix table |
| suffix `CO` | `... Connector` | high | Official GDOT suffix table |
| suffix `BY` | `... Bypass` | high | Official GDOT suffix table |
| suffix `LO` | `... Loop` | high | Official GDOT suffix table |
| suffix `AL` | `... Alternate` | high | Official GDOT suffix table |
| suffix `EA`, `WE`, `NO`, `SO` | directional subtype | high | Official GDOT suffix table |
| suffix `XL`, `XN`, `XS`, `XE`, `XW` | `... Express Lane ...` | high | Official GDOT suffix table |

## Recommended Phase 1 Implementation

Implement in the ETL now, not as an external analyst-only memo.

## Current Staged-Build Pulse Check

Applying the proposed rule set to the current `roadway_inventory.db` route IDs yields this provisional segment distribution:

| Proposed `ROUTE_FAMILY` | Segment count |
|---|---:|
| `Local/Other` | `512,941` |
| `U.S. Route` | `65,754` |
| `State Route` | `30,219` |
| `Interstate` | `13,341` |

Important interpretation note:

- The `U.S. Route` and `State Route` split above is still only as good as the Appendix G crosswalk and the encoded state-route number, so treat those two counts as planning-grade but not final shield inventory counts.

## Recommended Verification Follow-On

To raise `U.S. Route` versus `State Route` from medium to high confidence, use an official signed-route verification pass based on GDOT ArcWeb / statewide-viewer layers such as:

- `Interstates`
- `US Highway`
- `State Routes`
- `Statewide Roads`

That verification design is documented separately in:

- [Georgia Signed-Route Verification Strategy](./2026-04-07-georgia-signed-route-verification-strategy.md)

Current implementation note:

- an initial ETL scaffold is now wired for the next staged rebuild
- live reference matchers now cover GDOT `Interstates`, `US Highway`, and `State Routes`
- matching currently uses derived `RCLINK` candidates and then prefers milepoint overlap where the official layer exposes interval fields
- if one official reference is unavailable at runtime, the ETL continues with the others
- if all official references are unavailable at runtime, the ETL falls back to the baseline crosswalk result and logs warnings

### Pipeline changes

- Add a Georgia route-family config file under `02-Data-Staging/config/`.
- Add a dedicated classifier helper under `02-Data-Staging/scripts/01_roadway_inventory/`.
- Materialize these fields during normalization:
  - `BASE_ROUTE_NUMBER`
  - `ROUTE_SUFFIX_LABEL`
  - `ROUTE_FAMILY`
  - `ROUTE_FAMILY_DETAIL`
  - `ROUTE_FAMILY_CONFIDENCE`
  - `ROUTE_FAMILY_SOURCE`
- Keep existing parsed fields so the raw Georgia route-ID structure remains inspectable.
- Keep `FUNCTIONAL_CLASS`, `NHS`, and `STRAHNET` as separate dimensions.

### Recommended downstream use

- Use `ROUTE_FAMILY` for broad filtering, summary reporting, and map symbology.
- Use `ROUTE_FAMILY_DETAIL` for richer local-road and facility subtype reporting.
- Use `ROUTE_FAMILY_CONFIDENCE` to guard any reporting that tries to interpret `U.S. Route` vs `State Route` as an on-the-ground signed shield.

## Limitations and Ambiguous Cases

- The Georgia `ROUTE_ID` does not directly store a shield-family code such as `I`, `US`, or `GA`.
- Appendix G solves part of that problem, but not perfectly, because it is still a state-route crosswalk.
- U.S.-route classifications should therefore be treated as a GDOT-route-code interpretation, not as a fully verified shield inventory.
- Functional class still must come from `F_SYSTEM` or `FUNCTIONAL_CLASS`.
- Current staged data only contains `SYSTEM_CODE` values `1` and `2`, but live GDOT metadata documents `3` and `4` as valid future cases.
- Non-mainline route IDs can use special route-code layouts. The ETL should extract family-level route number before trying Interstate or U.S.-route matching.

## Texas Comparison Boundary

Texas is useful only as a reminder that local-road subtypes can matter.

Do not import Texas route-ID conventions into Georgia logic.

Georgia-specific truth for this crosswalk comes from GDOT route-guide, data-dictionary, and live-LRS sources only.
