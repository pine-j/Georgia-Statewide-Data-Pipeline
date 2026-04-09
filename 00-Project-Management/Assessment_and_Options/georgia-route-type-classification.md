# Georgia Route Type Classification

## Scope

This note defines a granular Georgia roadway route-type taxonomy for the GDOT roadway pipeline and maps every current segment in `02-Data-Staging/databases/roadway_inventory.db` to one code.

Snapshot used for this assessment:

- Database: `02-Data-Staging/databases/roadway_inventory.db`
- Segment count: `244,904`
- Assessment date: `2026-04-08`

Important implementation note:

- The current staged database was built before correcting the HPMS `RouteSigning` crosswalk in `hpms_enrichment.py`.
- The inventory tables below report the database as it exists today.
- The proposed `ROUTE_TYPE_GDOT` coverage counts below were computed by running the corrected classifier in-memory against the existing staged columns, without rerunning the full ETL.

## Source Hierarchy

Official sources used for the taxonomy:

- GDOT Understanding Route IDs guide: <https://www.dot.ga.gov/DriveSmart/Data/Documents/Guides/UnderstandingRouteIDs_Doc.pdf>
- GDOT Road Inventory Data Dictionary: <https://www.dot.ga.gov/DriveSmart/Data/Documents/Road_Inventory_Data_Dictionary.pdf>
- Local agent-friendly conversion of the GDOT data dictionary: `01-Raw-Data/Roadway-Inventory/GDOT_Road_Inventory/DataDictionary.agent.md`
- FHWA HPMS Field Manual, October 1, 2024: <https://omb.report/icr/202405-2125-003/doc/144173100.pdf>
- GDOT Road & Traffic Data landing page: <https://www.dot.ga.gov/GDOT/Pages/RoadTrafficData.aspx>

## Official GDOT And FHWA Taxonomy

### GDOT `ROUTE_ID` structure

The GDOT route guide and data dictionary both describe the 16-character `ROUTE_ID` as:

`FUNCTION_TYPE + COUNTY + SYSTEM_CODE + ROUTE_CODE + SUFFIX + DIRECTION`

Key consequences for classification:

- `FUNCTION_TYPE` is the official source for ramps, collector-distributors, frontage roads, alleys, managed facilities, and similar non-mainline facility types.
- `SYSTEM_CODE` separates state-system routes from public roads and future private/federal roads.
- `ROUTE_SUFFIX` carries signed subtype cues such as business, spur, alternate, connector, loop, and bypass.
- For `FUNCTION_TYPE` `2`-`4`, GDOT says route-code digits `6`-`8` are reference post and digits `9`-`11` are the underlying route number.

### GDOT function types

From the GDOT data dictionary and route guide:

| Code | Official meaning | Current rows |
| --- | --- | ---: |
| `1` | Main Line | 241,436 |
| `2` | Ramp | 3,160 |
| `3` | Collector Distributor | 173 |
| `4` | Ramp-CD Connector | 0 |
| `5` | Frontage Road | 135 |
| `6` | Alley | 0 |
| `7` | Separate Managed Facility | 0 |
| `8` | Local | 0 |
| `9` | Private | 0 |

### GDOT system codes

From the GDOT data dictionary and route guide:

| Code | Official meaning | Current rows |
| --- | --- | ---: |
| `1` | State Highway Route | 18,499 |
| `2` | Public Road | 226,405 |
| `3` | Private | 0 |
| `4` | Federal Route | 0 |

### GDOT common suffixes

The route guide's common suffix table includes:

| Code | Official meaning |
| --- | --- |
| `BU` | Business |
| `SP` | Spur |
| `AL` | Alternate |
| `CO` | Connector |
| `BY` | Bypass |
| `LO` | Loop |
| `SB` | South Business |
| `SE` | Spur East |
| `EA` | East |
| `WE` | West |
| `SO` | South |
| `NO` | North |
| `EC` | East Connector |
| `XL` / `XN` / `XS` / `XE` / `XW` | Express-lane variants |

Observed in current data:

- Official alpha suffixes present: `BU`, `CO`, `AL`, `SP`, `WE`, `SO`, `BY`, `LO`, `EA`, `SB`, `SE`, `EC`, `NO`
- Rare undecoded alpha/mixed suffixes also appear, mostly on ramps or a handful of mainline rows: `AS`, `CW`, `CB`, `BB`, `DA`, `AA`, `BA`, etc.
- Numeric public-road suffixes `01`-`99` are GDOT local/city codes, not signed shield subtypes.

### FHWA HPMS `RouteSigning`

The current codebase's old `ROUTE_SIGNING_MAP` treated `1` as Interstate. That is not the FHWA standard.

The HPMS manual defines:

| Code | Official meaning | Observed rows |
| --- | --- | ---: |
| `1` | Not Signed | 208,122 |
| `2` | Interstate | 1,057 |
| `3` | U.S. | 4,976 |
| `4` | State | 8,981 |
| `5` | Off-Interstate Business Marker | 0 |
| `6` | County | 0 |
| `7` | Township | 0 |
| `8` | Municipal | 0 |
| `9` | Parkway Marker or Forest Route Marker | 0 |
| `10` | Other | 0 |

Important consequence:

- `HPMS_ROUTE_SIGNING = 1` should be interpreted as `Not Signed`, not `Interstate`.
- For this pipeline, HPMS should only upgrade signed family when the code is `2`, `3`, `4`, or `5`.
- The HPMS `RouteQualifier` field would help distinguish business/alternate/bypass/spur directly, but it is not currently staged in the database. That is why subtype still has to come from GDOT `FUNCTION_TYPE` and `ROUTE_SUFFIX`.

## What The Current Staged Data Actually Contains

### Main observations

- The current roadway inventory only contains `SYSTEM_CODE` `1` and `2`. No private (`3`) or federal (`4`) rows are present in the staged snapshot.
- Only four `FUNCTION_TYPE` values are present: mainline, ramp, collector-distributor, and frontage road.
- Public roads split cleanly into county roads versus city streets by mainline suffix:
  - `SYSTEM_CODE = 2` and suffix `00` -> county road
  - `SYSTEM_CODE = 2` and numeric suffix `01`-`99` -> city street
- Signed state-system subtypes already appear in the GDOT route structure:
  - `Business`, `Connector`, `Spur`, `Alternate`, `Bypass`, `Loop`
  - `Ramp`, `Collector Distributor`, `Frontage Road`
- The current coarse `ROUTE_TYPE` field is not useful for granular classification. It just mirrors `SYSTEM_CODE`:
  - `1 = State Highway Route`
  - `2 = Public Road`

### Inventory tables

#### Function type

```text
1 | Main Line | 241436
2 | Ramp | 3160
3 | Collector Distributor | 173
5 | Frontage Road | 135
```

#### System code

```text
2 | Public Road | 226405
1 | State Highway Route | 18499
```

#### Existing coarse route-family detail

```text
Local/Other | County Road | 146946
Local/Other | City Street | 79345
U.S. Route | U.S. Route | 8340
State Route | State Route | 4616
Interstate | Interstate Ramp | 2370
Interstate | Interstate | 1114
U.S. Route | U.S. Route Ramp | 637
U.S. Route | U.S. Route Business | 304
Interstate | Interstate Collector Distributor | 157
U.S. Route | U.S. Route Connector | 156
U.S. Route | U.S. Route Alternate | 144
U.S. Route | U.S. Route Spur | 106
State Route | State Route Frontage Road | 94
Local/Other | Public Road Ramp | 90
U.S. Route | U.S. Route South | 69
State Route | State Route Ramp | 63
U.S. Route | U.S. Route West | 62
State Route | State Route Connector | 47
U.S. Route | U.S. Route Bypass | 47
State Route | State Route Spur | 28
Local/Other | Public Road Frontage Road | 24
State Route | State Route West | 20
U.S. Route | U.S. Route Loop | 17
U.S. Route | U.S. Route Collector Distributor | 16
U.S. Route | U.S. Route Frontage Road | 16
Interstate | Interstate Spur | 14
State Route | State Route Bypass | 14
State Route | State Route Alternate | 13
U.S. Route | U.S. Route South Business | 10
U.S. Route | U.S. Route East | 7
State Route | State Route South | 6
Interstate | Interstate Connector | 3
State Route | State Route East | 3
U.S. Route | U.S. Route Spur East | 3
Interstate | Interstate Frontage Road | 1
U.S. Route | U.S. Route East Connector | 1
U.S. Route | U.S. Route North | 1
```

#### Raw HPMS `RouteSigning` values in the staged DB

```text
1.0 | 208122
4.0 | 8981
3.0 | 4976
2.0 | 1057
```

#### Functional system

```text
7.0 | Local | 196530
5.0 | Major Collector | 7230
4.0 | Minor Arterial | 4590
1.0 | Interstate | 2823
6.0 | Minor Collector | 2710
3.0 | Principal Arterial - Other | 897
2.0 | Principal Arterial - Other Freeways and Expressways | 636
```

#### Facility type

```text
2.0 | Two-Way (non-restricted) | 217510
4.0 | Non Mainline | 3284
1.0 | One-Way (non-restricted) | 1251
6.0 | Planned/Unbuilt | 15
```

#### Existing `ROUTE_TYPE` field

```text
2 | Public Road | 226405
1 | State Highway Route | 18499
```

## Proposed `ROUTE_TYPE_GDOT` Taxonomy

The goal is a single HSYS-style code per segment. Subtype takes precedence over family. That means:

- Interstate ramps are `RP`, not `I`
- U.S. route business segments are `BU`, not `US`
- State route frontage roads are `FR`, not `SR`

### Core codes used by the current data

| Code | Label | Determining fields | Current rows |
| --- | --- | --- | ---: |
| `I` | Interstate Mainline | `HPMS_ROUTE_SIGNING in (2,5)` and no function/suffix override | 1,070 |
| `US` | U.S. Highway Mainline | `HPMS_ROUTE_SIGNING = 3` or signed-family fallback and no override | 4,758 |
| `SR` | State Route Mainline | `HPMS_ROUTE_SIGNING = 4` or signed-family fallback and no override | 8,413 |
| `CR` | County Road | `SYSTEM_CODE = 2`, `FUNCTION_TYPE = 1`, `ROUTE_SUFFIX = 00` | 146,939 |
| `CS` | City Street | `SYSTEM_CODE = 2`, `FUNCTION_TYPE = 1`, numeric `ROUTE_SUFFIX 01-99` | 79,345 |
| `BU` | Business U.S. Highway | suffix `BU` or `SB` and signed family `U.S. Route` | 240 |
| `BS` | Business State Route | suffix `BU` or `SB` and signed family `State Route` | 74 |
| `SP` | Spur Route | suffix `SP` or `SE` and no function override | 151 |
| `CN` | Connector Route | suffix `CO`, `EC`, or `CW` and no function override | 209 |
| `LP` | Loop Route | suffix `LO` and no function override | 17 |
| `AL` | Alternate Route | suffix `AL` or `AS` and no function override | 159 |
| `BY` | Bypass Route | suffix `BY` and no function override | 61 |
| `RP` | Ramp | `FUNCTION_TYPE = 2` | 3,160 |
| `CD` | Collector-Distributor | `FUNCTION_TYPE = 3` | 173 |
| `FR` | Frontage Road | `FUNCTION_TYPE = 5` | 135 |

### Supported official/future codes not present in the current snapshot

| Code | Label | Determining fields | Current rows |
| --- | --- | --- | ---: |
| `BI` | Business Interstate | suffix `BU` or `SB` and signed family `Interstate`, or future HPMS code `5` business marker | 0 |
| `RC` | Ramp-CD Connector | `FUNCTION_TYPE = 4` | 0 |
| `ML` | Managed/Express Lane | `FUNCTION_TYPE = 7` or suffix `XL/XN/XS/XE/XW` | 0 |
| `ALY` | Alley | `FUNCTION_TYPE = 6` | 0 |
| `YC` | Y-Connector / Local Connector | `FUNCTION_TYPE = 8` | 0 |
| `PR` | Private Road | `SYSTEM_CODE = 3` or `FUNCTION_TYPE = 9` | 0 |
| `FED` | Federal Route | `SYSTEM_CODE = 4` | 0 |
| `OT` | Other / Unclassified | terminal fallback only | 0 |

### How the generic subtype codes distribute by signed family

These subtype codes are intentionally family-agnostic, with the family preserved in `HWY_NAME`.

| Route type | Interstate | U.S. Route | State Route |
| --- | ---: | ---: | ---: |
| `SP` | 14 | 109 | 28 |
| `CN` | 3 | 159 | 47 |
| `LP` | 0 | 17 | 0 |
| `AL` | 0 | 146 | 13 |
| `BY` | 0 | 47 | 14 |

## Coverage Analysis

Standalone run of the new classifier over the current `segments` table:

```text
AL | 159
BS | 74
BU | 240
BY | 61
CD | 173
CN | 209
CR | 146939
CS | 79345
FR | 135
I | 1070
LP | 17
RP | 3160
SP | 151
SR | 8413
US | 4758
```

Coverage notes:

- All `244,904` rows classified.
- `ROUTE_TYPE_GDOT` nulls: `0`
- `HWY_NAME` nulls: `0`
- `HWY_NAME = OTHER ROUTE`: `0`
- State-system rows total: `18,499`
- State-system rows with explicit HPMS signed-family codes `2`-`5`: `15,007`
- State-system rows without HPMS signing: `3,492`
- Of those `3,492` fallback rows, only `244` are non-`RP`/`CD`/`FR` rows. Most of the fallback burden is on ramps and collector-distributors where `FUNCTION_TYPE` is already enough to classify.

## `HWY_NAME` derivation

Rules implemented:

- `I` / `BI`
  - Use `HPMS_ROUTE_NUMBER` first when present.
  - Else decode GDOT's internal Interstate route numbers with the official 400-series crosswalk.
  - Example: `401 -> I-75`, `402 -> I-20`, `408 -> I-475`, `421 -> I-516`.
- `US` / `BU`
  - Use `HPMS_ROUTE_NUMBER` when present.
  - Else parse route numbers embedded in `HPMS_ROUTE_NAME` such as `27AL`, `25BU`, `301BY`.
  - Else use a canonical number lookup across the same route base and signed-family prefix.
  - Final fallback is `BASE_ROUTE_NUMBER`.
- `SR` / `BS` / `SP` / `CN` / `LP` / `AL` / `BY`
  - Use the parsed or canonical route number when available.
  - Otherwise use `BASE_ROUTE_NUMBER`.
- `CR` / `CS`
  - Use `BASE_ROUTE_NUMBER` to create a stable synthetic identifier such as `CR-123` or `CS-6830`.
  - This is not a posted street name; it is a stable local-road route key derived from GDOT's own numbering.
- `RP` / `CD` / `FR`
  - Use generic names: `RAMP`, `COLLECTOR-DISTRIBUTOR`, `FRONTAGE RD`

Residual risk:

- Georgia `ROUTE_ID` values encode administrative state-route numbers, not shield family.
- That means the final `US` fallback to `BASE_ROUTE_NUMBER` is only a last resort and can still reflect GDOT's internal numbering on a small uncovered subset.
- In the current standalone run, only `244` state-system non-`RP`/`CD`/`FR` rows lack explicit HPMS signing, so this residual risk is limited.

Examples from the standalone run:

- `I-20`
- `US-441`
- `SR-29`
- `US-23 BUS`
- `SR-21 BUS`
- `US-25 BYP`
- `SR-20 SPUR`
- `SR-3 CONN`
- `CR-3467`
- `CS-6830`

## Implementation Changes

Code changes made without rerunning the full pipeline:

- `02-Data-Staging/scripts/01_roadway_inventory/hpms_enrichment.py`
  - Corrected FHWA HPMS `RouteSigning` interpretation so only codes `2`, `3`, `4`, and `5` upgrade signed family.
- `02-Data-Staging/scripts/01_roadway_inventory/route_type_gdot.py`
  - New classifier that derives `ROUTE_TYPE_GDOT` and `HWY_NAME`.
- `02-Data-Staging/scripts/01_roadway_inventory/normalize.py`
  - Added the new classifier after HPMS enrichment and before label decoding.
- `02-Data-Staging/config/roadway_domain_labels.json`
  - Added `route_type_gdot` label domain.
  - Added missing `FUNCTION_TYPE = 6` label (`Alley`).
- `05-RAPTOR-Integration/states/Georgia/categories/Roadways.py`
  - Added `ROUTE_TYPE_GDOT`, `ROUTE_TYPE_GDOT_LABEL`, and `HWY_NAME` to `COLUMNS_TO_KEEP`.
- `02-Data-Staging/scripts/01_roadway_inventory/validate.py`
  - Added presence and coverage checks for `ROUTE_TYPE_GDOT`, `ROUTE_TYPE_GDOT_LABEL`, and `HWY_NAME`.

## Recommended Review Focus

Before a full rebuild, the highest-value review questions are:

- Whether `I` is the preferred mainline code, or whether the project wants `INT`
- Whether `CN`, `SP`, `AL`, `BY`, and `LP` should stay family-agnostic like Texas HSYS, or be split into family-specific variants
- Whether `CR-123` / `CS-6830` is acceptable for local-road `HWY_NAME`, or whether local roads should keep a generic `COUNTY ROAD` / `CITY STREET`
- Whether rare undecoded suffixes such as `AS` and `CW` should remain mapped to `AL` / `CN`, or be held for a later appendix-I extraction pass

## Appendix A: Raw Query Output

### `ROUTE_SUFFIX`

<details>
<summary>All distinct route suffix values observed in the staged DB</summary>

```text
00 |  | 161140
03 |  | 15733
01 |  | 13047
05 |  | 9880
07 |  | 7683
09 |  | 6532
13 |  | 4642
11 |  | 4074
17 |  | 2993
15 |  | 2872
23 |  | 1795
25 |  | 1494
31 |  | 1170
21 |  | 996
19 |  | 933
A0 |  | 684
C0 |  | 662
27 |  | 621
D0 |  | 616
B0 |  | 597
33 |  | 544
02 |  | 316
BU | Business | 304
04 |  | 219
CO | Connector | 206
06 |  | 172
29 |  | 169
10 |  | 168
08 |  | 159
AL | Alternate | 157
SP | Spur | 148
12 |  | 141
14 |  | 113
16 |  | 104
F0 |  | 92
20 |  | 82
WE | West | 82
G0 |  | 81
H0 |  | 81
E0 |  | 79
SO | South | 75
18 |  | 73
BY | Bypass | 61
22 |  | 60
24 |  | 60
45 |  | 59
56 |  | 58
30 |  | 56
36 |  | 56
26 |  | 55
32 |  | 55
50 |  | 54
28 |  | 48
35 |  | 48
70 |  | 47
95 |  | 46
40 |  | 45
34 |  | 44
37 |  | 42
54 |  | 41
46 |  | 40
51 |  | 39
39 |  | 38
60 |  | 38
80 |  | 38
90 |  | 38
38 |  | 37
43 |  | 36
47 |  | 36
49 |  | 36
55 |  | 36
59 |  | 36
61 |  | 36
41 |  | 35
42 |  | 35
52 |  | 35
58 |  | 35
48 |  | 34
57 |  | 34
66 |  | 34
67 |  | 34
65 |  | 33
71 |  | 33
87 |  | 33
44 |  | 32
53 |  | 32
78 |  | 32
68 |  | 31
86 |  | 31
98 |  | 31
64 |  | 30
69 |  | 30
72 |  | 30
74 |  | 30
81 |  | 30
85 |  | 30
89 |  | 30
96 |  | 30
CB |  | 30
62 |  | 29
63 |  | 29
77 |  | 29
82 |  | 29
84 |  | 29
S0 |  | 29
79 |  | 28
88 |  | 28
91 |  | 28
97 |  | 28
75 |  | 27
76 |  | 27
83 |  | 27
BB |  | 27
73 |  | 26
92 |  | 26
93 |  | 26
94 |  | 26
DA |  | 25
99 |  | 21
AA |  | 21
BA |  | 21
M0 |  | 20
LO | Loop | 18
AB |  | 17
CA |  | 17
EA | East | 16
DB |  | 15
SB | South Business | 14
J0 |  | 13
K0 |  | 13
T0 |  | 13
L0 |  | 9
FB |  | 8
DC |  | 6
HB |  | 6
Q0 |  | 6
V0 |  | 6
FA |  | 5
GA |  | 5
HA |  | 5
KA |  | 5
SA |  | 5
SC |  | 5
SE | Spur East | 5
U0 |  | 5
AC |  | 4
BC |  | 4
GB |  | 4
P0 |  | 4
PB |  | 4
AX |  | 3
B1 |  | 3
BX |  | 3
C1 |  | 3
CC |  | 3
CX |  | 3
DX |  | 3
A1 |  | 2
AS |  | 2
CD |  | 2
CE |  | 2
CW |  | 2
D1 |  | 2
DE |  | 2
EB |  | 2
EC | East Connector | 2
FC |  | 2
HC |  | 2
LA |  | 2
LB |  | 2
MB |  | 2
ME |  | 2
N0 |  | 2
RC |  | 2
TB |  | 2
TC |  | 2
 |  | 1
AE |  | 1
BD |  | 1
BE |  | 1
CF |  | 1
HE |  | 1
JB |  | 1
KB |  | 1
LE |  | 1
MO |  | 1
NB |  | 1
NO | North | 1
PA |  | 1
R0 |  | 1
RB |  | 1
TA |  | 1
UB |  | 1
UC |  | 1
VC |  | 1
W0 |  | 1
```

</details>

### `ROUTE_FAMILY x ROUTE_SUFFIX`

<details>
<summary>Observed family/detail/suffix combinations</summary>

```text
Interstate | Interstate Ramp |  | 2359
Interstate | Interstate |  | 1114
Interstate | Interstate Collector Distributor |  | 157
Interstate | Interstate Spur | Spur | 14
Interstate | Interstate Ramp | South Business | 4
Interstate | Interstate Connector | Connector | 3
Interstate | Interstate Ramp | East | 3
Interstate | Interstate Ramp | Spur East | 2
Interstate | Interstate Frontage Road |  | 1
Interstate | Interstate Ramp | East Connector | 1
Interstate | Interstate Ramp | Loop | 1
Local/Other | County Road |  | 146946
Local/Other | City Street |  | 79345
Local/Other | Public Road Ramp |  | 90
Local/Other | Public Road Frontage Road |  | 24
State Route | State Route |  | 4616
State Route | State Route Frontage Road |  | 94
State Route | State Route Ramp |  | 63
State Route | State Route Connector | Connector | 47
State Route | State Route Spur | Spur | 28
State Route | State Route West | West | 20
State Route | State Route Bypass | Bypass | 14
State Route | State Route Alternate | Alternate | 13
State Route | State Route South | South | 6
State Route | State Route East | East | 3
U.S. Route | U.S. Route |  | 8340
U.S. Route | U.S. Route Ramp |  | 634
U.S. Route | U.S. Route Business | Business | 304
U.S. Route | U.S. Route Connector | Connector | 156
U.S. Route | U.S. Route Alternate | Alternate | 144
U.S. Route | U.S. Route Spur | Spur | 106
U.S. Route | U.S. Route South | South | 69
U.S. Route | U.S. Route West | West | 62
U.S. Route | U.S. Route Bypass | Bypass | 47
U.S. Route | U.S. Route Loop | Loop | 17
U.S. Route | U.S. Route Collector Distributor |  | 16
U.S. Route | U.S. Route Frontage Road |  | 16
U.S. Route | U.S. Route South Business | South Business | 10
U.S. Route | U.S. Route East | East | 7
U.S. Route | U.S. Route Ramp | East | 3
U.S. Route | U.S. Route Spur East | Spur East | 3
U.S. Route | U.S. Route East Connector | East Connector | 1
U.S. Route | U.S. Route North | North | 1
```

</details>

### Signed-route verification source mix in the current staged DB

```text
Local/Other | route_id_crosswalk | 226405
U.S. Route | gdot_us_highway | 5516
State Route | route_id_crosswalk | 4671
U.S. Route | route_id_crosswalk | 4653
Interstate | route_id_crosswalk | 2585
Interstate | gdot_interstates | 1074
```

### Top 50 `HPMS_ROUTE_NAME` values

<details>
<summary>Top 50 observed `HPMS_ROUTE_NAME` strings</summary>

```text
Unknown; | 312
Church St; | 298
Main St; | 260
Pine St; | 204
Oak St; | 181
Railroad St; | 167
Fall Line Freeway; | 129
2nd St; | 128
1st St; | 127
Lra; | 125
Broad St; | 120
County Line Rd; | 120
College St; | 119
Lakeview Dr; | 118
3rd St; | 113
Lee St; | 110
Maple St; | 109
Airport Rd; | 105
Cherry St; | 100
Poplar St; | 100
River Rd; | 100
4th St; | 99
Elm St; | 98
Washington St; | 94
27AL | 93
Interstate 285; | 92
2nd Ave; | 91
Green St; | 90
Spring St; | 90
Unnamed; | 90
Davis Rd; | 88
Jackson St; | 87
Ridge Rd; | 86
Williams Rd; | 86
Dogwood Dr; | 85
Jones Rd; | 85
Hill St; | 80
Williams St; | 79
3rd Ave; | 78
1st Ave; | 76
Central Ave; | 76
Georgia Ave; | 75
Martin Luther King Jr Dr; | 75
Smith St; | 73
Walnut St; | 73
5th St; | 72
Jefferson St; | 72
Park St; | 72
Woodland Dr; | 72
Johnson Rd; | 70
```

</details>
