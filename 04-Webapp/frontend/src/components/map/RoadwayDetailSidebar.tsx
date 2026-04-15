import CloseRoundedIcon from "@mui/icons-material/CloseRounded";
import {
  Box,
  CircularProgress,
  Divider,
  IconButton,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableRow,
  Tooltip,
  Typography,
} from "@mui/material";

import { RoadwayDetail } from "../../types/api";

/** Human-readable label + tooltip description for each raw attribute key. */
interface AttributeMeta {
  label: string;
  description: string;
}

const ATTRIBUTE_LABELS: Record<string, AttributeMeta> = {
  ROUTE_ID: { label: "Route ID", description: "Official route identifier assigned by GDOT" },
  ROUTE_FAMILY: { label: "Route Family", description: "Route classification family (Interstate, US, State, Local)" },
  HWY_NAME: { label: "Highway Name", description: "Signed highway name or street name" },
  FUNCTIONAL_CLASS: { label: "Functional Class", description: "Federal Highway Administration functional classification" },
  functional_class_viz: { label: "Functional Class (Display)", description: "Simplified functional class used for map display" },
  AADT: { label: "AADT", description: "Annual Average Daily Traffic — the average number of vehicles per day over a full year (2024)" },
  FUTURE_AADT_2044: { label: "Future AADT (2044)", description: "Projected annual average daily traffic for the year 2044" },
  TRUCK_AADT: { label: "Truck AADT", description: "Annual average daily truck traffic count (2024)" },
  PCT_SADT: { label: "% Single-Unit AADT", description: "Percentage of AADT attributable to single-unit trucks" },
  PCT_CADT: { label: "% Combo-Unit AADT", description: "Percentage of AADT attributable to combination (multi-unit) trucks" },
  K_FACTOR: { label: "K-Factor", description: "Ratio of 30th highest hourly volume to AADT, used for design hour volume" },
  D_FACTOR: { label: "D-Factor", description: "Directional distribution factor — percentage of peak-hour traffic in the predominant direction" },
  VMT: { label: "VMT", description: "Vehicle Miles Traveled — total miles driven on this segment annually" },
  NUM_LANES: { label: "Number of Lanes", description: "Total number of through lanes in both directions" },
  SPEED_LIMIT: { label: "Speed Limit", description: "Posted speed limit in miles per hour" },
  MEDIAN_TYPE: { label: "Median Type", description: "Type of median separating opposing traffic (e.g., raised, depressed, none)" },
  HWY_DES: { label: "Highway Design", description: "Highway design designation derived from lanes, median, route family, and access control" },
  SURFACE_TYPE: { label: "Surface Type", description: "Road surface material type (e.g., asphalt, concrete)" },
  FACILITY_TYPE: { label: "Facility Type", description: "Type of road facility (e.g., one-way, two-way)" },
  NHS_IND: { label: "NHS Indicator", description: "Whether this segment is part of the National Highway System" },
  OWNERSHIP: { label: "Ownership", description: "Agency or entity that owns the roadway" },
  SYSTEM_CODE: { label: "System Code", description: "Road system classification code" },
  DIRECTION: { label: "Direction", description: "Direction of travel for the segment" },
  COUNTY_CODE: { label: "County Code", description: "FIPS county code where the segment is located" },
  county_all: { label: "All Counties", description: "All counties this segment passes through" },
  DISTRICT: { label: "District", description: "GDOT district number" },
  DISTRICT_LABEL: { label: "District Name", description: "GDOT district name and number" },
  LENGTH_MILES: { label: "Length (Miles)", description: "Segment length in miles" },
  HPMS_ACCESS_CONTROL: { label: "Access Control", description: "HPMS access control type (full, partial, or no control)" },
  SEC_EVAC: { label: "Evacuation Route", description: "Whether this segment is on a GDOT-designated hurricane evacuation route" },
  SEC_EVAC_CONTRAFLOW: { label: "Contraflow Route", description: "Whether this segment is on a GDOT hurricane evacuation contraflow corridor" },
  SEC_EVAC_ROUTE_NAME: { label: "Evacuation Route Name", description: "Name(s) of the evacuation route(s) overlapping this segment" },
  TRUCK_PCT: { label: "Truck %", description: "Percentage of total traffic that is trucks" },
  FROM_MILEPOINT: { label: "Begin Milepoint", description: "Starting milepoint of the segment along the route" },
  TO_MILEPOINT: { label: "End Milepoint", description: "Ending milepoint of the segment along the route" },
  SINGLE_UNIT_AADT_2024: { label: "Single-Unit Truck AADT", description: "Annual average daily count of single-unit trucks (2024)" },
  COMBO_UNIT_AADT_2024: { label: "Combo-Unit Truck AADT", description: "Annual average daily count of combination trucks (2024)" },
  URBAN_CODE: { label: "Urban Area Code", description: "Census urban area code for the segment location" },
  unique_id: { label: "Unique ID", description: "Internal unique segment identifier" },
};

/** Labels that end with _label are display-friendly versions of coded columns. */
const LABEL_SUFFIX_PAIRS: Record<string, string> = {
  MEDIAN_TYPE: "median_type_label",
  SURFACE_TYPE: "surface_type_label",
  NHS_IND: "nhs_ind_label",
  OWNERSHIP: "ownership_label",
  FACILITY_TYPE: "facility_type_label",
  SYSTEM_CODE: "system_code_label",
  DIRECTION: "direction_label",
};

/** Keys that are redundant or internal — always hidden. */
const HIDDEN_KEYS = new Set([
  "id",
  "unique_id",
  "road_name",
  "COUNTY_CODE",
  "DISTRICT",
  "DISTRICT_LABEL",
  "ROUTE_ID",
  "HWY_NAME",
  "county_all",
  // Raw coded columns when a _label version exists
  "MEDIAN_TYPE",
  "SURFACE_TYPE",
  "NHS_IND",
  "OWNERSHIP",
  "FACILITY_TYPE",
  "SYSTEM_CODE",
  "DIRECTION",
  // Duplicate traffic columns
  "SINGLE_UNIT_AADT_2024",
  "COMBO_UNIT_AADT_2024",
  "AADT_2024",
  "FUTURE_AADT",
  "CURRENT_ADT",
  "ADT",
  // Source / intermediate columns dropped from pipeline output
  "START_M",
  "END_M",
  "RouteId",
  "StateID",
  "BeginDate",
  "Comments",
  "Shape_Length",
  "BeginPoint",
  "EndPoint",
  "GDOT_District",
  "F_SYSTEM",
  "THROUGH_LANES",
  "NHS",
  "URBAN_ID",
  "COUNTY",
  "PARSED_SYSTEM_CODE",
  "PARSED_ROUTE_NUMBER",
  "PARSED_SUFFIX",
  "PARSED_DIRECTION",
  "PARSED_COUNTY_CODE",
  "PARSED_FUNCTION_TYPE",
]);

/** Ordered list of attribute keys we want to show first. Unlisted keys appear after. */
const DISPLAY_ORDER: string[] = [
  "AADT",
  "FUTURE_AADT_2044",
  "TRUCK_AADT",
  "TRUCK_PCT",
  "PCT_SADT",
  "PCT_CADT",
  "K_FACTOR",
  "D_FACTOR",
  "VMT",
  "FUNCTIONAL_CLASS",
  "NUM_LANES",
  "SPEED_LIMIT",
  "HWY_DES",
  "ROUTE_FAMILY",
  "median_type_label",
  "surface_type_label",
  "facility_type_label",
  "nhs_ind_label",
  "ownership_label",
  "system_code_label",
  "direction_label",
  "HPMS_ACCESS_CONTROL",
  "SEC_EVAC",
  "SEC_EVAC_CONTRAFLOW",
  "SEC_EVAC_ROUTE_NAME",
  "LENGTH_MILES",
  "FROM_MILEPOINT",
  "TO_MILEPOINT",
  "URBAN_CODE",
];

interface AttributeRow {
  key: string;
  label: string;
  description: string;
  value: string;
}

function formatValue(value: string | number | boolean | null): string {
  if (value === null || value === undefined || value === "") return "N/A";
  if (typeof value === "boolean") return value ? "Yes" : "No";
  if (typeof value === "number") {
    if (Number.isInteger(value)) return value.toLocaleString();
    return value.toLocaleString(undefined, { maximumFractionDigits: 2 });
  }
  return String(value);
}

function getLabel(key: string): string {
  if (ATTRIBUTE_LABELS[key]) return ATTRIBUTE_LABELS[key].label;
  // Convert snake_case / SCREAMING_SNAKE to Title Case
  return key
    .replace(/_/g, " ")
    .replace(/\b\w/g, (c) => c.toUpperCase())
    .replace(/\bAadt\b/gi, "AADT")
    .replace(/\bVmt\b/gi, "VMT")
    .replace(/\bNhs\b/gi, "NHS")
    .replace(/\bHpms\b/gi, "HPMS")
    .replace(/\bPct\b/gi, "%");
}

function getDescription(key: string): string {
  return ATTRIBUTE_LABELS[key]?.description ?? `Raw attribute: ${key}`;
}

function buildDisplayRows(attributes: Record<string, string | number | boolean | null>): AttributeRow[] {
  const seen = new Set<string>();
  const rows: AttributeRow[] = [];

  // Prefer _label versions over coded columns
  for (const [codedKey, labelKey] of Object.entries(LABEL_SUFFIX_PAIRS)) {
    if (labelKey in attributes && codedKey in attributes) {
      seen.add(codedKey);
    }
  }

  // Deduplicate: track values we've already shown to skip true duplicates
  const shownValues = new Map<string, string>();

  const addRow = (key: string) => {
    if (seen.has(key) || HIDDEN_KEYS.has(key)) return;
    seen.add(key);
    const value = attributes[key];
    const formatted = formatValue(value);

    // Skip if another key already showed the exact same label+value
    const label = getLabel(key);
    const dedupeKey = `${label.toLowerCase()}::${formatted.toLowerCase()}`;
    if (shownValues.has(dedupeKey)) return;
    shownValues.set(dedupeKey, key);

    rows.push({ key, label, description: getDescription(key), value: formatted });
  };

  // Add in preferred order first
  for (const key of DISPLAY_ORDER) {
    if (key in attributes) addRow(key);
  }

  // Then add remaining keys
  for (const key of Object.keys(attributes)) {
    if (!seen.has(key)) addRow(key);
  }

  return rows;
}

function displayDistrictLabel(label: string): string {
  const separatorIndex = label.indexOf(" - ");
  return separatorIndex >= 0 ? label.slice(separatorIndex + 3) : label;
}

function formatCountyMeta(county: string, countyAll?: string | null): string {
  if (!countyAll) return `${county} County`;
  const normalized = countyAll.trim();
  if (!normalized || normalized.toLowerCase() === county.trim().toLowerCase()) {
    return `${county} County`;
  }
  return `Counties: ${normalized}`;
}

// ── Sections for visual grouping ──

interface AttributeSection {
  title: string;
  keys: Set<string>;
}

const SECTIONS: AttributeSection[] = [
  {
    title: "Traffic",
    keys: new Set(["AADT", "FUTURE_AADT_2044", "TRUCK_AADT", "TRUCK_PCT", "PCT_SADT", "PCT_CADT", "K_FACTOR", "D_FACTOR", "VMT"]),
  },
  {
    title: "Road Characteristics",
    keys: new Set(["FUNCTIONAL_CLASS", "functional_class_viz", "NUM_LANES", "SPEED_LIMIT", "HWY_DES", "ROUTE_FAMILY", "median_type_label", "surface_type_label", "facility_type_label", "nhs_ind_label", "HPMS_ACCESS_CONTROL", "SEC_EVAC", "SEC_EVAC_CONTRAFLOW", "SEC_EVAC_ROUTE_NAME"]),
  },
  {
    title: "Administration",
    keys: new Set(["ownership_label", "system_code_label", "direction_label", "URBAN_CODE"]),
  },
  {
    title: "Segment Geometry",
    keys: new Set(["LENGTH_MILES", "FROM_MILEPOINT", "TO_MILEPOINT"]),
  },
];

function groupRowsIntoSections(rows: AttributeRow[]): { title: string; rows: AttributeRow[] }[] {
  const assigned = new Set<string>();
  const sections: { title: string; rows: AttributeRow[] }[] = [];

  for (const section of SECTIONS) {
    const sectionRows = rows.filter((r) => section.keys.has(r.key));
    if (sectionRows.length > 0) {
      sections.push({ title: section.title, rows: sectionRows });
      sectionRows.forEach((r) => assigned.add(r.key));
    }
  }

  const remaining = rows.filter((r) => !assigned.has(r.key));
  if (remaining.length > 0) {
    sections.push({ title: "Other", rows: remaining });
  }

  return sections;
}

// ── Component ──

interface RoadwayDetailSidebarProps {
  detail: RoadwayDetail | null;
  isLoading: boolean;
  hasError: boolean;
  onClose: () => void;
}

export function RoadwayDetailSidebar({ detail, isLoading, hasError, onClose }: RoadwayDetailSidebarProps) {
  const countyAll = detail
    ? typeof detail.attributes.county_all === "string" ? detail.attributes.county_all : null
    : null;

  const rows = detail ? buildDisplayRows(detail.attributes) : [];
  const sections = groupRowsIntoSections(rows);

  return (
    <Box
      sx={{
        width: { xs: "100%", lg: 380 },
        maxWidth: "100%",
        height: "100%",
        display: "flex",
        flexDirection: "column",
        bgcolor: "#ffffff",
        borderLeft: "1px solid rgba(17, 61, 73, 0.12)",
        overflow: "hidden",
      }}
    >
      {/* Header */}
      <Box
        sx={{
          px: 2,
          py: 1.5,
          background: "linear-gradient(180deg, #f8fbfb 0%, #f0f5f6 100%)",
          borderBottom: "1px solid rgba(17, 61, 73, 0.1)",
          flexShrink: 0,
        }}
      >
        <Stack direction="row" alignItems="flex-start" justifyContent="space-between" spacing={1}>
          <Box sx={{ minWidth: 0 }}>
            <Typography variant="subtitle1" sx={{ fontWeight: 700, color: "#10232f", lineHeight: 1.3 }}>
              {detail?.road_name ?? "Loading..."}
            </Typography>
            {detail && (
              <Typography variant="body2" sx={{ color: "#47626b", mt: 0.25, fontSize: "0.8rem" }}>
                {formatCountyMeta(detail.county, countyAll)} | {displayDistrictLabel(detail.district_label)}
              </Typography>
            )}
          </Box>
          <IconButton size="small" onClick={onClose} sx={{ mt: -0.25 }}>
            <CloseRoundedIcon fontSize="small" />
          </IconButton>
        </Stack>
      </Box>

      {/* Body */}
      {isLoading && (
        <Box sx={{ display: "grid", placeItems: "center", flex: 1 }}>
          <Stack spacing={1} alignItems="center">
            <CircularProgress size={28} />
            <Typography variant="body2" color="text.secondary">Loading segment details...</Typography>
          </Stack>
        </Box>
      )}

      {hasError && !isLoading && (
        <Box sx={{ display: "grid", placeItems: "center", flex: 1 }}>
          <Typography variant="body2" color="error">Segment details could not be loaded.</Typography>
        </Box>
      )}

      {detail && !isLoading && (
        <TableContainer sx={{ flex: 1, minHeight: 0, overflowY: "auto" }}>
          {sections.map((section, sIdx) => (
            <Box key={section.title}>
              <Box
                sx={{
                  px: 2,
                  py: 0.75,
                  bgcolor: "#f3f6f7",
                  borderBottom: "1px solid rgba(17, 61, 73, 0.08)",
                  position: "sticky",
                  top: 0,
                  zIndex: 1,
                }}
              >
                <Typography variant="caption" sx={{ fontWeight: 700, letterSpacing: 0.5, color: "#47626b", textTransform: "uppercase", fontSize: "0.68rem" }}>
                  {section.title}
                </Typography>
              </Box>
              <Table size="small">
                <TableBody>
                  {section.rows.map((row) => (
                    <Tooltip key={row.key} title={row.description} placement="left" arrow enterDelay={400}>
                      <TableRow
                        hover
                        sx={{
                          "&:last-child td": { borderBottom: 0 },
                          cursor: "default",
                        }}
                      >
                        <TableCell
                          sx={{
                            fontWeight: 600,
                            fontSize: "0.75rem",
                            color: "#4c626a",
                            width: 160,
                            py: 0.75,
                            whiteSpace: "nowrap",
                          }}
                        >
                          {row.label}
                        </TableCell>
                        <TableCell
                          sx={{
                            fontSize: "0.8rem",
                            color: "#10232f",
                            py: 0.75,
                            wordBreak: "break-word",
                          }}
                        >
                          {row.value}
                        </TableCell>
                      </TableRow>
                    </Tooltip>
                  ))}
                </TableBody>
              </Table>
              {sIdx < sections.length - 1 && <Divider />}
            </Box>
          ))}
        </TableContainer>
      )}
    </Box>
  );
}
