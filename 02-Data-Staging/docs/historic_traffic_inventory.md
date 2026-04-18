# GDOT Historic Traffic Data Inventory (2010–2023)

Read-only inventory of `01-Raw-Data/Roadway-Inventory/GDOT_Traffic/Traffic_Historical.zip` conducted 2026-04-18 for Phase 2 of the AADT historic station→segment modeling effort. See `C:/Users/adith/.claude/plans/aadt-historic-station-segment-model.md` for the downstream plan.

Archive: 563 MB zip, 256 entries, one folder per year under `2010_thr_2023_Published_Traffic/`. 14 annual drops total.

---

## 1. Summary matrix

Legend: `Y` = present, `.` = not present, `~` = partial/degraded, row counts are exact.

| Year | Segment layer | Station-point layer | AADT xlsx | Stats-Type field on segments | Stats-Type field on stations | Seg rows | TC rows | Station xlsx rows | Segment CRS |
|-----:|:-------------:|:-------------------:|:---------:|:----------------------------:|:-----------------------------:|---------:|--------:|------------------:|:------------|
| 2010 | Y (shp)       | Y (shp)             | Y         | .                            | Y (`Estimated` E/A)           |   23 355 |  20 063 |            20 063 | EPSG:4326   |
| 2011 | Y (shp)       | Y (shp)             | Y         | .                            | Y (`Estimated` E/A)           |   22 391 |  20 063 |            20 063 | EPSG:4326   |
| 2012 | Y (shp)       | Y (shp)             | Y         | .                            | Y (`Estimated` E/A)           |   22 929 |  20 063 |            20 063 | EPSG:4326   |
| 2013 | Y (shp)       | Y (shp)             | Y         | .                            | Y (`Estimated` E/A)           |   29 751 |  32 822 |            32 822 | EPSG:4326   |
| 2014 | Y (shp)       | Y (shp)             | Y         | Y (`Actual_Est` E/A)         | Y (`Actual_Est` E/A)          |   26 142 |  26 142 |            26 142 | GA Lambert  |
| 2015 | Y (shp)       | Y (shp) + extra shp | Y         | .                            | . (no flag)                   |  222 416 |  26 699 |            26 699 | EPSG:4326   |
| 2016 | Y (shp)       | Y (shp)             | Y (.xls)  | .                            | .                             |   94 258 |  26 542 |            26 898 | GA Lambert  |
| 2017 | Y (shp)       | Y (shp)             | Y (×2)    | .                            | Y (`Statistics type` words)   |   54 290 |  26 542 |            26 923 | GA Lambert  |
| 2018 | Y (shp)       | Y (shp)             | Y (×3)    | .                            | Y (`Statistics type` words)   |   87 870 |  27 582 |            26 925 | GA Lambert  |
| 2019 | Y (shp)       | Y (shp)             | Y (×2)    | .                            | Y (`Statistics`)              |  272 343 |  25 879 |            25 879 | EPSG:4326   |
| 2020 | Y (gdb)       | .                   | Y (×2)    | .                            | Y (`Statistics type`)         |  311 785 |       . |            25 889 | EPSG:4326   |
| 2021 | .             | .                   | Y (×2)    | .                            | Y (`Statistics type`)         |        . |       . |            25 966 | —           |
| 2022 | .             | .                   | Y (×2)    | .                            | Y (`Statistics type`)         |        . |       . |            25 668 | —           |
| 2023 | .             | .                   | Y (×2)    | .                            | Y (`Statistics type`)         |        . |       . |            25 714 | —           |

**Headline findings**

1. Segment-level AADT geometry exists for **2010–2020 (11 years)**; 2021–2023 are station-level only.
2. Station-point geometry exists for **2010–2019 (10 years)**; 2020+ moved station coordinates into the xlsx as `Latitude/Longitude` columns.
3. Annual station-count xlsx exists for **every year 2010–2023 (14 years)**; 2017+ schema is uniform and matches the 2024 `Statistics_Type` naming.
4. 2024's `Statistics_Type` field appears **only** on segments for 2014 (as `Actual_Est`). For all other years, the Actual/Estimated flag lives on the station side, not on segments.
5. 2014 is the biggest schema outlier — different field names, Georgia Statewide Lambert CRS, TC-anchored schema (BEG_MP/END_MP/RCLINK).
6. Segment row-counts jump sharply in 2015, 2019, 2020 (>200k rows), consistent with the network expanding from state-highway–only to all-public-road coverage. 2010–2013 (~25k rows) appear to be state-highway subset.

---

## 2. Per-year detail

File names shown are inside `2010_thr_2023_Published_Traffic/<year-folder>/`.

### 2010 — `2010_Published_AADT/`
- **Traffic_Data_GA_2010.shp** — LineString, EPSG:4326, 23 355 features, 18 fields. HPMS-style. No station linker, no Actual/Estimated flag.
- **TC_Locations_2010.shp** — Point, EPSG:4326, 20 063 features, 25 fields. `TC_Num` is the station ID; `AADT_2010` holds station AADT; `Estimated` carries E/A codes (12 513 E, 7 550 A). Embedded truck-pct trio for 2010/2011/2012.
- **2010_all_site_aadts.xlsx** — 20 063 rows, 25 cols. Mirror of TC_Locations_2010.shp attribute table; `TC_Num`, `AADT_2010`, `Estimated`, Lat/Long, and rolling truck% 2010–2012.

Segment fields: `Year_Recor, State_Code, Route_ID, Begin_Poin, End_Point, AADT_VN, AADT_COMBI, AADT_SINGL, COUNTY_COD, DIR_FACTOR, F_SYSTEM_V, FACILITY_T, FUTURE_AAD, K_FACTOR_V, PCT_PEAK_C, PCT_PEAK_S, URBAN_CODE, Shape_Leng`.

### 2011 — `2011_Published_AADT/`
- **Traffic_Data_GA_2011.shp** — 22 391 features, 18 fields, same HPMS schema as 2010.
- **TC_Locations_2011.shp** — 20 063 features, 25 fields, same schema as 2010 with `AADT_2011` replacing `AADT_2010`.
- **2011_all_site_aadts.xlsx** — 20 063 rows, same column set.

### 2012 — `2012_Published_AADT/`
- **Traffic_Data_GA_2012.shp** — 22 929 features, 18 fields, identical schema to 2010–2011.
- **TC_Locations_2012.shp** — 20 063 features.
- **2012_all_site_aadts.xlsx** — 20 063 rows. `AADT_2012` is int64 (others float64) — harmless but note the dtype drift.

> Row counts for 2010/2011/2012 TC are identical at 20 063 — the same station dbf appears to have been copied across the three drops; only the `AADT_YYYY` column changes.

### 2013 — `2013_Published_AADT/`
- **Traffic_Data_GA_2013.shp** — 29 751 features, 18-field HPMS schema.
- **TC_Locations_2013.shp** — 32 822 features, **only 12 fields** — schema slimmed vs 2010–2012 (lost the truck% trio). `TC_Number` replaces `TC_Num`. `Estimated` E/A flag retained (19 758 E, 13 064 A).
- **2013_published_aadt_all.xlsx** — 32 822 rows, 12 cols. Full mirror of TC_Locations_2013.shp.
- **2013_traffic_counts.zip** — nested shp, 7 923 Point features, 10 fields (`OBJECTID, TC_Number, Year_, Site_Type, AADT, Route_Numb, Begin_Mile, End_Mile, Lattitude, Longitude`). Appears to be actual-count sites only (subset of TC_Locations).

### 2014 — `2014_Published_AADT/` (**schema outlier**)
- **Traffic_Data_GA_2014.shp** — LineString, **Georgia Statewide Lambert (ESRI:102604)**, 26 142 features, 21 fields. Completely different schema from every other year: uses `RCLINK, BEG_MP, END_MP, AADT, Actual_Est, Pct_Truck, K_Factor, Dir_Factor, Single_Uni, Combinatio, AADT_Singl, AADT_Combi, Pct_Peak_S, Pct_Peak_C, lat, long, TC, TC_NUM`. **This is the only year with `Actual_Est` on segments** (17 373 E, 8 769 A).
- **TC_Locations_2014.shp** — Point, EPSG:4326, 26 142 features, 20 fields — identical attribute set to the segment layer minus `Shape_Leng`.
- **2014_Published_AADT.xlsx** — 26 142 rows, 21 cols (adds `Parent_Chi`).
- **2014_All_Traffic_Counters.zip** — nested shp, 26 142 LineStrings, 23 fields (superset adding `OBJECTID, Parent_Chi, LOC_ERROR`).

Red flag: 2014's `TC_NUM` is the only pre-2016 segment-level station linker; reconciling this schema with neighbouring years requires rename + reproject.

### 2015 — `2015_Published_AADT/`
- **Traffic_Data_GA_2015.shp** — LineString, EPSG:4326, **222 416 features** (≈10× any prior year), 18-field HPMS schema identical to 2010–2013. Row-count jump suggests expansion to all-public-road network.
- **TC_Locations_2015.shp** — Point, EPSG:4326, 26 699 features, **9 fields only** (`Station_ID, Coordinate, Year_, aadt, aadtt, k30, d30, Latitude, Longitude`). No Actual/Estimated flag. `aadtt` = truck AADT.
- **AADT_2015_TC/AADT_2015_TC.shp** — Bonus LineString layer, 16 679 features, 14 fields keyed to HPMS data items (`DATA_ITEM, VALUE_NUME, VALUE_TEXT, VALUE_DATE, LAST_MD_BY/ON, DATA_SOURC`) — appears to be raw submittal log.
- **2015_end_of_year_all_tcs.xlsx** — 26 699 rows, 7 cols.

### 2016 — `2016_Published_Traffic/`
- **Traffic_Data_GA_2016.shp** — LineString Z, Georgia Statewide Lambert, 94 258 features, 16 fields: `OBJECTID, ROUTE_ID, FROM_MILEP, TO_MILEPOI, COUNTY_COD, TC_NUMBER, AADT, AADT_SINGL, PCT_PEAK_S, AADT_COMBI, PCT_PEAK_C, K_FACTOR, D_Factor, FUTURE_AAD, Shape_Leng, Shape_Le_1`. **First year with `TC_NUMBER` on segments** — but 56.2% null (53 019/94 258).
- **TC_Locations_2016.shp** — Point Z, Lambert, 26 542 features, 6 fields only (`ROUTE_ID, MILEPOINT, TC_NUMBER, MEASURED_L, MEASURED_1, ROUTE_KEY`) — purely a locator, **no AADT on this layer** (must join to xlsx).
- **Traffic_2016_Tables/Traffic_Published_2016.csv** — 94 258 rows, 16 cols, 1:1 mirror of the segment dbf with `COUNTY_NAME` and `Lat/Long` columns added.
- **Traffic_2016_Tables/All_Stations_Lat_Long_2016.xls** — 26 898 rows, 9 cols (`OBJECTID, TC_Number, siteclientid, road, description, datatype, classifier, latitude, longitude`). This is the 2016 station master.

No `Statistics type`/`Actual_Est` anywhere in 2016.

### 2017 — `2017_Published_Traffic/`
- **Traffic_Data_GA_2017.shp** — LineString Z, Lambert, 54 290 features, 17 fields. Mostly HPMS style back: `Route_ID, Begin_Poin, End_Point, AADT_VN, AADT_COMBI, AADT_SINGL, COUNTY_COD, DIR_FACTOR, F_SYSTEM_V, FUTURE_AAD, K_FACTOR_V, PCT_PEAK_C, PCT_PEAK_S, URBAN_CODE, COUNTY_NAM, TC_NUMBER, Shape_Leng`. `TC_NUMBER` present — 43.0% null (23 326/54 290).
- **TC_Locations_2017.shp** — Point Z, Lambert, 26 542 features, same 6-field locator schema as 2016. No AADT on the layer.
- **2017 Annual Statistics.xlsx** — 26 923 rows, 14 cols. **First year with `Statistics type` as a word (Estimated/Actual/Calculated)**: 19 203 Estimated, 6 312 Actual, 930 NaN, 478 Calculated. Also has Station ID, K/D-Factor, `Future AADT`, `Station Type` — matches 2024 xlsx schema.
- **All Station Annual Statistics_2017.xlsx** — 26 923 rows, 23 cols (rolling AADT/Truck% back to 2008).

### 2018 — `2018_Published_Traffic/`
- **Traffic_Data_GA_2018.shp** — LineString Z, Lambert, 87 870 features, 19 fields. HPMS + `TC_NUMBER` (38.0% null) + `COUNTY_NAM`. Has a duplicated `OBJECTID_1` and dropped `K_FACTOR_V` (only year missing K-factor on segments).
- **TC_Locations_2018.shp** — Point Z, Lambert, 27 582 features, 5 fields (`ROUTE_ID, MILEPOINT, TC_NUMBER, MEASURED_L, MEASURED_1`). Lost `ROUTE_KEY`.
- **aadt_and_truckpct_2018.xlsx** — 26 925 rows, 23 cols (rolling back to 2009).
- **aadt_and_truckpct_2018_No duplicate TCs.xlsx** — 26 896 rows, same 23 cols, + 58-row "Sheet1" dup-list with a `Comment` column.
- **annualized_statistics_2018.xlsx** — 26 925 rows, 16 cols. Adds `Unnamed: 4` empty column between `Long` and `Year`. Statistics type populated.

Red flag: `Thumbs.db` and the duplicate `OBJECTID_1` suggest messy producer workflow.

### 2019 — `2019_Published_Traffic/`
- **Traffic_Data_GA_2019.shp** — LineString, EPSG:4326, **272 343 features**, **42 fields**. Every HPMS attribute is expanded into a triple: numeric value + a text `_1` column + a `datetime64` `_2` column (likely a per-attribute QA date stamp). DBF is 928 MB — largest file in the archive.
- **TC_Locations_2019.shp** — Point, EPSG:4326, 25 879 features, 16 fields — schema converges toward 2024: `Station_ID, Functional, Lat_Long, Year, AADT, Statistics, Single_Uni, Combo_Unit, PCT_Peak_S, PCT_Peak_C, K_Factor, D_Factor, Future_AAD, Station_Ty, Latitude, Longitude`. `Statistics` values: 15 928 Estimated, 9 509 Actual, 442 Calculated.
- **aadt_and_truckpct_2019_07242020.xlsx** — 27 022 rows, 23 cols.
- **annualized_statistics_2019_07242020.xlsx** — 25 879 rows, 14 cols, matches 2017 xlsx schema.

Red flag: the 42-field segment layer's `_2` datetime columns have no obvious use for AADT modeling — collapse to the 19 HPMS base fields during staging.

### 2020 — `2020_Published_Traffic/` (**file geodatabase, not shp**)
- Folder is an **unnamed Esri File Geodatabase**. The gdb tables live directly in the year folder (no `.gdb` suffix). Had to copy the `a00000*.gdb*` files into a `.gdb`-named temp folder to open with GDAL.
- **Traffic_Data_2020** (only feature class) — MultiLineString, EPSG:4326, **311 785 features**, 19 fields: `Year_Recor, State_Code, Route_ID, Begin_Poin, End_Point, AADT_VN, AADT_COMBI, AADT_SINGL, COUNTY_COD, DIR_FACTOR, F_SYSTEM_V, FACILITY_T, FUTURE_AAD, K_FACTOR_V, NHS_VN, PCT_PEAK_C, PCT_PEAK_S, URBAN_CODE, Shape_Length`. Cleanest HPMS schema in the archive; `NHS_VN` is new.
- **2020_aadt_and_truckpct.xlsx** — 27 170 rows, 23 cols.
- **2020_annualized_statistics.xlsx** — 25 889 rows, 14 cols. `Statistics type` populated.
- **No station-point feature class** exists. Station coordinates must come from the xlsx `Lat/Long` column (text format "lat,long").

### 2021 — `2021_Published_Traffic/`
- **No spatial data.** xlsx only.
- **2021 aadt_and_truckpct (64).xlsx** — 27 228 rows, 23 cols.
- **2021 annualized_statistics (85).xlsx** — 25 966 rows, 14 cols. `Statistics type` populated.

### 2022 — `2022_Published_Traffic/`
- **No spatial data.** xlsx only.
- **2022 aadt_and_truckpct.xlsx** — 27 271 rows, **24 cols** (split `Lat/Long` into separate `Latitude`, `Longitude` numeric columns — first year with that).
- **2022 annualized_statistics.xlsx** — 25 668 rows, 15 cols.

### 2023 — `2023_Published_Traffic/`
- **No spatial data.** xlsx only.
- **aadt_and_truckpct_2023.xlsx** — 27 299 rows, 24 cols (same Lat/Long split as 2022).
- **annualized_statistics_2023.xlsx** — 25 714 rows, 15 cols. `Statistics type`: 17 401 Estimated, 7 864 Actual, 449 Calculated.

---

## 3. Cross-year schema drift catalog

### 3a. Segment AADT layer — field presence matrix

`Y` = field present, blank = absent. `—` = year has no segment layer.

| field (truncated to 10 chars) | 2010 | 2011 | 2012 | 2013 | 2014 | 2015 | 2016 | 2017 | 2018 | 2019 | 2020 |
|---|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
| Year_Recor | Y | Y | Y | Y |   | Y |   |   |   | Y | Y |
| State_Code | Y | Y | Y | Y |   | Y |   |   |   | Y | Y |
| Route_ID   | Y | Y | Y | Y |   | Y | Y¹| Y | Y | Y | Y |
| Begin_Poin | Y | Y | Y | Y |   | Y |   | Y | Y | Y | Y |
| End_Point  | Y | Y | Y | Y |   | Y |   | Y | Y | Y | Y |
| FROM_MILEP |   |   |   |   |   |   | Y |   |   |   |   |
| TO_MILEPOI |   |   |   |   |   |   | Y |   |   |   |   |
| BEG_MP     |   |   |   |   | Y |   |   |   |   |   |   |
| END_MP     |   |   |   |   | Y |   |   |   |   |   |   |
| RCLINK     |   |   |   |   | Y |   |   |   |   |   |   |
| AADT_VN    | Y | Y | Y | Y |   | Y |   | Y | Y | Y | Y |
| AADT       |   |   |   |   | Y |   | Y |   |   |   |   |
| AADT_COMBI | Y | Y | Y | Y |   | Y | Y | Y | Y | Y | Y |
| AADT_SINGL | Y | Y | Y | Y |   | Y | Y | Y | Y | Y | Y |
| COUNTY_COD | Y | Y | Y | Y |   | Y | Y | Y | Y | Y | Y |
| COUNTY_NAM |   |   |   |   |   |   |   | Y | Y |   |   |
| COUNTY     |   |   |   |   | Y |   |   |   |   |   |   |
| DIR_FACTOR | Y | Y | Y | Y |   | Y |   | Y | Y | Y | Y |
| Dir_Factor |   |   |   |   | Y |   |   |   |   |   |   |
| D_Factor   |   |   |   |   |   |   | Y |   |   |   |   |
| K_FACTOR_V | Y | Y | Y | Y |   | Y |   | Y |   | Y | Y |
| K_Factor   |   |   |   |   | Y |   |   |   |   |   |   |
| K_FACTOR   |   |   |   |   |   |   | Y |   |   |   |   |
| F_SYSTEM_V | Y | Y | Y | Y |   | Y |   | Y | Y | Y | Y |
| FACILITY_T | Y | Y | Y | Y |   | Y |   |   |   | Y | Y |
| FUTURE_AAD | Y | Y | Y | Y |   | Y | Y | Y | Y | Y | Y |
| PCT_PEAK_C | Y | Y | Y | Y | Y²| Y | Y | Y | Y | Y | Y |
| PCT_PEAK_S | Y | Y | Y | Y | Y²| Y | Y | Y | Y | Y | Y |
| URBAN_CODE | Y | Y | Y | Y |   | Y |   | Y | Y | Y | Y |
| NHS_VN     |   |   |   |   |   |   |   |   |   |   | Y |
| TC_NUMBER  |   |   |   |   |   |   | Y | Y | Y |   |   |
| TC_NUM     |   |   |   |   | Y |   |   |   |   |   |   |
| Actual_Est |   |   |   |   | Y |   |   |   |   |   |   |
| Pct_Truck  |   |   |   |   | Y |   |   |   |   |   |   |
| Single_Uni |   |   |   |   | Y |   |   |   |   |   |   |
| Combinatio |   |   |   |   | Y |   |   |   |   |   |   |
| AADT_Singl |   |   |   |   | Y |   |   |   |   |   |   |
| AADT_Combi |   |   |   |   | Y |   |   |   |   |   |   |
| RCLINK     |   |   |   |   | Y |   |   |   |   |   |   |
| lat / long |   |   |   |   | Y |   |   |   |   |   |   |
| AADT_V{D,T,1,2} / *_1 / *_2 |   |   |   |   |   |   |   |   |   | Y |   |
| OBJECTID   |   |   |   |   |   |   | Y |   | Y |   |   |

¹ 2016 stores Route-ID as `ROUTE_ID` (uppercase) alongside `FROM_MILEPOINT/TO_MILEPOINT`.
² 2014 uses `Pct_Peak_S / Pct_Peak_C` (mixed case) — different from the `PCT_PEAK_S/C` used elsewhere.

### 3b. Station-point layer — field presence matrix (condensed)

| field | 2010 | 2011 | 2012 | 2013 | 2014 | 2015 | 2016 | 2017 | 2018 | 2019 |
|---|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
| station-ID: TC_Num         | Y | Y | Y |   |   |   |   |   |   |   |
| station-ID: TC_Number      |   |   |   | Y |   |   | Y | Y | Y |   |
| station-ID: TC / TC_NUM    |   |   |   |   | Y |   |   |   |   |   |
| station-ID: Station_ID     |   |   |   |   |   | Y |   |   |   | Y |
| AADT_YYYY (year-stamped)   | Y | Y | Y |   |   |   |   |   |   |   |
| AADT                       |   |   |   | Y | Y |   |   |   |   | Y |
| aadt (lowercase)           |   |   |   |   |   | Y |   |   |   |   |
| Estimated (E/A)            | Y | Y | Y | Y |   |   |   |   |   |   |
| Actual_Est (E/A)           |   |   |   |   | Y |   |   |   |   |   |
| Statistics (word)          |   |   |   |   |   |   |   |   |   | Y |
| Latitude / Longitude       |   |   |   |   |   | Y |   |   |   | Y |
| Lattitude [sic] / Longitude| Y | Y | Y | Y |   |   |   |   |   |   |
| lat / long_                |   |   |   |   | Y |   |   |   |   |   |
| ROUTE_ID + MILEPOINT       |   |   |   |   |   |   | Y | Y | Y |   |
| MEASURED_L / MEASURED_1    |   |   |   |   |   |   | Y | Y | Y |   |
| K_Factor / D_Factor        |   |   |   |   | Y |   |   |   |   | Y |
| Single_Unit / Combo_Unit   |   |   |   |   | Y |   |   |   |   | Y |
| K30 / D30 (short form)     |   |   |   |   |   | Y |   |   |   |   |
| aadtt (truck aadt)         |   |   |   |   |   | Y |   |   |   |   |

### 3c. Station xlsx (annualized_statistics family) — converges 2017 onward

| field | 2010-12 | 2013 | 2014 | 2015 | 2016 | 2017 | 2018 | 2019 | 2020 | 2021 | 2022 | 2023 |
|---|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
| TC_Num / TC Number / Station ID | Y (TC_Num) | Y (TC Number) | Y (TC_NUM) | Y (Station ID) | Y (TC_Number) | Y (Station ID) | Y | Y | Y | Y | Y | Y |
| Functional Class             |  |  |  |  |  | Y | Y | Y | Y | Y | Y | Y |
| Lat/Long (text)              |  |  |  |  |  | Y | Y | Y | Y | Y |  |  |
| Latitude / Longitude (num)   |  |  |  |  | Y |  |  |  |  |  | Y | Y |
| Year                         |  | Y | Y | Y |  | Y | Y | Y | Y | Y | Y | Y |
| AADT                         |  | Y | Y | Y |  | Y | Y | Y | Y | Y | Y | Y |
| Statistics type              |  |  | Y (E/A)¹ |  |  | Y (words) | Y | Y | Y | Y | Y | Y |
| Single-Unit / Combo-Unit AADT|  |  |  |  |  | Y | Y | Y | Y | Y | Y | Y |
| % Peak SU / CU Trucks        |  |  |  |  |  | Y | Y | Y | Y | Y | Y | Y |
| K-Factor / D-Factor          |  |  |  |  |  | Y | Y | Y | Y | Y | Y | Y |
| Future AADT                  |  |  |  |  |  | Y | Y | Y | Y | Y | Y | Y |
| Station Type                 |  |  |  |  |  | Y | Y | Y | Y | Y | Y | Y |

¹ 2014 xlsx uses `Actual_Est` (E/A) rather than `Statistics type`.

### 3d. CRS drift

| Year | Segment CRS | Station CRS |
|-----:|:------------|:------------|
| 2010–2013 | EPSG:4326 | EPSG:4326 |
| 2014 | **ESRI:102604 (GA Statewide Lambert, US-ft)** | EPSG:4326 |
| 2015 | EPSG:4326 | EPSG:4326 (Lambert on bonus `AADT_2015_TC.shp`) |
| 2016–2018 | **ESRI:102604** | **ESRI:102604** |
| 2019 | EPSG:4326 | EPSG:4326 |
| 2020 | EPSG:4326 | — |
| 2021–2023 | — | — (xlsx only) |

Any cross-year spatial join must reproject 2014 and 2016–2018 layers.

---

## 4. Data-quality red flags

1. **TC_NUMBER on segment layers is heavily null** (2016: 56.2% null, 2017: 43.0%, 2018: 38.0%). A segment-side station ID is unreliable as a join key — use spatial join or the station xlsx as the authoritative index.
2. **`Actual_Est` / `Estimated` uses two different code systems**: 2010–2014 use one-letter codes `E`/`A`; 2017–2023 xlsx use full words `Estimated`/`Actual`/`Calculated`. 2024 uses the word form. Any training set needs a normaliser. 2015–2016 and 2019–2020 **segments** have no flag at all.
3. **2014 is a complete schema outlier** — different field naming convention, different CRS, and TC-anchored segment schema. Staging this year requires per-column renames + reprojection + E/A recoding.
4. **2019 segment layer has 42 fields with triple expansion** of each HPMS attribute into `<name>`, `<name>_1` (text), `<name>_2` (datetime64) — looks like a QA export artifact. The `_2` datetime columns add no modeling value; collapse to the base numeric column.
5. **2020 file geodatabase is missing the `.gdb` suffix** on its folder and has no station layer. To read it, the folder must be renamed to end in `.gdb`.
6. **2015, 2019, 2020 row counts balloon** (222k / 272k / 311k) vs ~25k in 2010–2013 — network scope likely switched from state-highway only to all-public-road. Comparing AADT totals across these boundaries without segmenting by `F_SYSTEM_V` / `URBAN_CODE` will produce spurious "growth".
7. **2016 `TC_Locations` and 2017–2018 `TC_Locations` have no AADT value on the point layer** — only a route/milepoint locator. Station AADT must come from the xlsx for those years.
8. **Duplicate/messy producer artifacts**: 2018 has `OBJECTID_1`, a `Thumbs.db`, two versions of the aadt_and_truckpct file; 2021 and 2022 xlsx names contain trailing `(64)`/`(85)` artifacts from some download tool. Not blocking, but staging code should not depend on exact xlsx filenames.
9. **`Lattitude` spelled with two t's** in TC_Locations_2010–2013 and the nested 2013 counts layer. Rename during staging.
10. **2010–2012 TC_Locations feature count (20 063)** is identical across all three years — the same dbf was republished with a rotating `AADT_YYYY` column rather than re-geocoded per year. Station coordinates should be treated as 2010-vintage for those three years.

---

## 5. Answer to Q11 — viable training split for the 2024 → historic station→segment model

The 2024 model is trained on `TRAFFIC_DataYear2024` where each segment carries `TC_NUMBER`, `TC_Latitude`, `TC_Longitude`, `AADT`, `Statistics_Type`, and the truck/K/D factors. To train or validate the same model on historic years, each year needs: (a) a segment geometry with AADT, (b) a station coordinate, and (c) — for the most rigorous training — a per-record Actual/Estimated flag so the model is fit only to measured ground truth.

**Tiered viability:**

- **Tier A — full segment-level modeling (11 years)**: 2010–2020 have segment geometries with AADT. Station coordinates are available for all 11 years (point shapefile 2010–2019, xlsx `Lat/Long` for 2020). These can all be used for station→segment matching experiments.

- **Tier B — model fit on *measured* ground truth only (9 years)**: Segment + AADT + a Statistics/Actual-Est flag somewhere in the data drop:
  - 2010–2013: flag on station side (E/A codes) — requires spatial join station→segment to label segments.
  - 2014: flag on segment side (`Actual_Est` E/A) — direct, but alien schema.
  - 2017–2020: flag on station xlsx (Estimated/Actual/Calculated) — requires xlsx→segment join on `TC_NUMBER` or spatial nearest-neighbour.
  - 2015, 2016: **no Statistics_Type flag anywhere** — unsafe for supervised fit, use for held-out test only.

- **Tier C — station-level validation only (3 years)**: 2021–2023 have station xlsx (with Statistics type) but no segment geometry. Cannot validate segment predictions directly; useful for checking station-level AADT forecasts.

**Recommended split for the Phase-2 modeling plan:**

- **Train (measured): 2017, 2018, 2019, 2020** — four years with the cleanest Statistics-type-word schema matching 2024 (same token values Estimated/Actual/Calculated on the xlsx, same factor set, station xlsx coord format). ≈8k–9k "Actual" stations × 4 = ~35k training rows.
- **Validation: 2010, 2011, 2012, 2013, 2014** — five years where E/A must be recoded but are schema-different enough to stress the normaliser. ≈30k–50k "Actual" stations total across the five years.
- **Holdout: 2024** — current target. Already staged in `TRAFFIC_Data_2024.gdb`.
- **Excluded from fit: 2015, 2016** — no Statistics flag; keep for sanity-check plots only.
- **Station-level only: 2021–2023** — no segment ground truth. Feed into a station-level temporal-consistency check, not the spatial model.

**Bottom line**: yes, the model can be trained and validated without joining years that lack segment data — the 11 years 2010–2020 cover segments and stations, and the 9 years with a Statistics/Actual-Est flag (2010–2014, 2017–2020) give enough labelled training + validation volume.

---

## 6. Recommended next step for historic staging

Before writing any staging code, run a schema-normalization spike that lands every segment-year into a single unified HPMS-aligned schema matching `TRAFFIC_DataYear2024`: field renames (e.g. `AADT_VN → AADTRound`, `AADT_COMBI → Combo_Unit_AADT`, `AADT_SINGL → Single_Unit_AADT`, `Begin_Poin → FROM_MILEPOINT`, `End_Point → TO_MILEPOINT`, `K_FACTOR_V → K_Factor`, `DIR_FACTOR → D_Factor`), dtype harmonisation (float32→float64), CRS reprojection for 2014 and 2016–2018, and `Actual_Est` / `Estimated` → `Statistics_Type` recoding (`E → Estimated`, `A → Actual`). Start staging with the 2017–2020 block because their schemas are closest to 2024 and they carry the needed Statistics-type flag on the accompanying xlsx (which joins cleanly to the segment `TC_NUMBER` where it's non-null). Treat 2014 and 2019 as separate staging modules — 2014 because of its Lambert CRS and outlier schema, 2019 because of the 42-field QA-stamped layout that needs column collapse. Defer 2015–2016 (no Statistics flag) and 2021–2023 (no geometry) to a later pass once the supervised fit pipeline is working.
