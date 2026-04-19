import { useEffect, useRef } from "react";

import { DEFAULT_HIGHWAY_TYPES, useAppStore } from "../store/useAppStore";

/**
 * Lightweight URL <-> store synchronization for the 9 admin-geography
 * selection arrays plus include_unincorporated.
 *
 * Wire this hook once (from AppShell) so the app is a no-op elsewhere.
 * On mount, reads the current URL and seeds the store (overriding any
 * defaults for keys present in the URL). After that, every relevant
 * store change rewrites the URL via history.replaceState - no
 * navigation, no scroll jump, no router dependency.
 *
 * Serialization matches services/api.ts buildFilterQuery repeated-
 * param convention: ?district=1&district=2&county=Fulton&city=101
 * plus include_unincorporated=true. Empty arrays mean "absent from URL".
 *
 * For highway types we DO NOT serialize the default set; only
 * deviations from DEFAULT_HIGHWAY_TYPES appear in the URL. A user who
 * deselects all three defaults sees the URL record that explicitly
 * (as highway_type=NONE) so reload reproduces their state.
 */

const FILTER_KEY_BY_STORE_FIELD = {
  selectedDistricts: "district",
  selectedCounties: "county",
  selectedHighwayTypes: "highway_type",
  selectedAreaOffices: "area_office",
  selectedMpos: "mpo",
  selectedRegionalCommissions: "regional_commission",
  selectedStateHouseDistricts: "state_house",
  selectedStateSenateDistricts: "state_senate",
  selectedCongressionalDistricts: "congressional",
  selectedCities: "city",
} as const;

type NumericStoreField = Exclude<
  keyof typeof FILTER_KEY_BY_STORE_FIELD,
  "selectedCounties" | "selectedHighwayTypes" | "selectedMpos"
>;

const NUMERIC_FIELDS: ReadonlySet<NumericStoreField> = new Set([
  "selectedDistricts",
  "selectedAreaOffices",
  "selectedRegionalCommissions",
  "selectedStateHouseDistricts",
  "selectedStateSenateDistricts",
  "selectedCongressionalDistricts",
  "selectedCities",
]);

const HIGHWAY_TYPE_NONE_SENTINEL = "NONE";

function readUrlParams(): URLSearchParams {
  if (typeof window === "undefined") {
    return new URLSearchParams();
  }
  return new URLSearchParams(window.location.search);
}

function writeUrlParams(params: URLSearchParams): void {
  if (typeof window === "undefined") {
    return;
  }
  const next = params.toString();
  const pathname = window.location.pathname;
  const hash = window.location.hash;
  const target = next ? `${pathname}?${next}${hash}` : `${pathname}${hash}`;
  // replaceState (not push) so the URL mirror doesn't pollute history on
  // every filter keystroke.
  if (window.location.pathname + window.location.search + window.location.hash !== target) {
    window.history.replaceState(window.history.state, "", target);
  }
}

function setsEqual<T>(a: readonly T[], b: readonly T[]): boolean {
  if (a.length !== b.length) {
    return false;
  }
  const seen = new Set<T>(a);
  return b.every((value) => seen.has(value));
}

export function useUrlSyncedFilters(): void {
  const state = useAppStore();
  const hasHydratedFromUrl = useRef(false);

  // One-shot hydration: on first mount, seed the store from URL.
  useEffect(() => {
    if (hasHydratedFromUrl.current) {
      return;
    }
    hasHydratedFromUrl.current = true;

    const params = readUrlParams();

    const getList = (paramName: string): string[] => params.getAll(paramName);

    for (const [field, paramName] of Object.entries(FILTER_KEY_BY_STORE_FIELD) as [
      keyof typeof FILTER_KEY_BY_STORE_FIELD,
      string,
    ][]) {
      const raw = getList(paramName);
      if (field === "selectedHighwayTypes") {
        if (raw.length === 0) {
          continue; // leave the store default intact
        }
        if (raw.length === 1 && raw[0] === HIGHWAY_TYPE_NONE_SENTINEL) {
          state.setSelectedHighwayTypes([]);
          continue;
        }
        state.setSelectedHighwayTypes(raw);
        continue;
      }
      if (field === "selectedCounties" || field === "selectedMpos") {
        if (raw.length === 0) continue;
        if (field === "selectedCounties") state.setSelectedCounties(raw);
        else state.setSelectedMpos(raw);
        continue;
      }
      if (NUMERIC_FIELDS.has(field as NumericStoreField)) {
        if (raw.length === 0) continue;
        const parsed = raw
          .map((value) => Number.parseInt(value, 10))
          .filter((value) => Number.isFinite(value));
        const setter = getNumericSetter(state, field as NumericStoreField);
        setter(parsed);
      }
    }

    if (params.get("include_unincorporated") === "true") {
      state.setIncludeUnincorporated(true);
    } else if (params.get("include_unincorporated") === "false") {
      state.setIncludeUnincorporated(false);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Reflective write: whenever any relevant slice changes, rewrite the URL.
  useEffect(() => {
    if (!hasHydratedFromUrl.current) {
      // Wait until after hydration so we don't clobber URL params on first render.
      return;
    }
    const params = new URLSearchParams(readUrlParams());

    // Clear all managed keys so removed selections drop out of the URL.
    for (const paramName of Object.values(FILTER_KEY_BY_STORE_FIELD)) {
      params.delete(paramName);
    }
    params.delete("include_unincorporated");

    const appendList = (paramName: string, values: readonly (string | number)[]) => {
      for (const value of values) {
        params.append(paramName, String(value));
      }
    };

    appendList("district", state.selectedDistricts);
    appendList("county", state.selectedCounties);
    if (
      state.selectedHighwayTypes.length === 0
        ? DEFAULT_HIGHWAY_TYPES.length > 0
        : !setsEqual(state.selectedHighwayTypes, DEFAULT_HIGHWAY_TYPES)
    ) {
      if (state.selectedHighwayTypes.length === 0) {
        params.append("highway_type", HIGHWAY_TYPE_NONE_SENTINEL);
      } else {
        appendList("highway_type", state.selectedHighwayTypes);
      }
    }
    appendList("area_office", state.selectedAreaOffices);
    appendList("mpo", state.selectedMpos);
    appendList("regional_commission", state.selectedRegionalCommissions);
    appendList("state_house", state.selectedStateHouseDistricts);
    appendList("state_senate", state.selectedStateSenateDistricts);
    appendList("congressional", state.selectedCongressionalDistricts);
    appendList("city", state.selectedCities);
    if (state.includeUnincorporated) {
      params.set("include_unincorporated", "true");
    }

    writeUrlParams(params);
  }, [
    state.selectedDistricts,
    state.selectedCounties,
    state.selectedHighwayTypes,
    state.selectedAreaOffices,
    state.selectedMpos,
    state.selectedRegionalCommissions,
    state.selectedStateHouseDistricts,
    state.selectedStateSenateDistricts,
    state.selectedCongressionalDistricts,
    state.selectedCities,
    state.includeUnincorporated,
  ]);
}

type NumericStoreSetter = (values: number[]) => void;

function getNumericSetter(
  state: ReturnType<typeof useAppStore.getState>,
  field: NumericStoreField,
): NumericStoreSetter {
  switch (field) {
    case "selectedDistricts":
      return state.setSelectedDistricts;
    case "selectedAreaOffices":
      return state.setSelectedAreaOffices;
    case "selectedRegionalCommissions":
      return state.setSelectedRegionalCommissions;
    case "selectedStateHouseDistricts":
      return state.setSelectedStateHouseDistricts;
    case "selectedStateSenateDistricts":
      return state.setSelectedStateSenateDistricts;
    case "selectedCongressionalDistricts":
      return state.setSelectedCongressionalDistricts;
    case "selectedCities":
      return state.setSelectedCities;
  }
}
