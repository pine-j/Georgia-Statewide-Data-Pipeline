import CheckBoxIcon from "@mui/icons-material/CheckBox";
import CheckBoxOutlineBlankIcon from "@mui/icons-material/CheckBoxOutlineBlank";
import {
  Autocomplete,
  AutocompleteRenderGroupParams,
  Box,
  Checkbox,
  TextField,
  Typography,
} from "@mui/material";

/**
 * Generic multi-select autocomplete for admin-geography filters. Extracted
 * from the original FiltersPanel district/county blocks so the 9 admin
 * filters (District, Area Office, County, MPO, Regional Commission, City,
 * State House, State Senate, Congressional) share one implementation.
 *
 * Responsibilities:
 *   - Render a checkbox-per-row multi-select MUI Autocomplete
 *   - Cascade: when `parentIds` + `getOptionParentId` are supplied,
 *     hide options whose parent isn't in the selection (e.g. Area
 *     Office cascades by selected districts)
 *   - Group: when `groupBy` is supplied, render sticky group headers
 *     (e.g. counties grouped by district label)
 *   - Pseudo-option: pin an "Unincorporated" row at the top of the
 *     city autocomplete. Visually distinct (italic label); its checkbox
 *     toggles a separate boolean state instead of the selected-ids array.
 *   - `renderTags={null}` matches the existing pattern of managing chip
 *     display externally (parent renders the selected chips).
 */
export interface PseudoOptionSpec {
  label: string;
  selected: boolean;
  onToggle: (selected: boolean) => void;
  helperText?: string;
}

export interface GeographyAutocompleteProps<
  Option,
  Id extends number | string,
> {
  label: string;
  placeholder?: string;
  helperText?: string;
  options: Option[];
  selected: readonly Id[];
  onChange: (selected: Id[]) => void;
  getOptionId: (option: Option) => Id;
  getOptionLabel: (option: Option) => string;
  /** Optional cascade by parent id (e.g. area office's parent_district). */
  parentIds?: readonly number[];
  getOptionParentId?: (option: Option) => number | null | undefined;
  /** Optional secondary cascade (e.g. city's parent_county). Applied in addition to parentIds. */
  secondaryParentIds?: readonly string[];
  getOptionSecondaryParentId?: (option: Option) => string | null | undefined;
  /** Optional group header function. */
  groupBy?: (option: Option) => string;
  /** Optional pseudo-option pinned above the real options (e.g. Unincorporated). */
  pseudoOption?: PseudoOptionSpec;
  size?: "small" | "medium";
}

export function GeographyAutocomplete<
  Option,
  Id extends number | string,
>({
  label,
  placeholder,
  helperText,
  options,
  selected,
  onChange,
  getOptionId,
  getOptionLabel,
  parentIds,
  getOptionParentId,
  secondaryParentIds,
  getOptionSecondaryParentId,
  groupBy,
  pseudoOption,
  size = "small",
}: GeographyAutocompleteProps<Option, Id>) {
  const filteredOptions = options.filter((option) => {
    if (parentIds && parentIds.length > 0 && getOptionParentId) {
      const parent = getOptionParentId(option);
      if (parent === null || parent === undefined) {
        return false;
      }
      if (!parentIds.includes(parent)) {
        return false;
      }
    }
    if (
      secondaryParentIds &&
      secondaryParentIds.length > 0 &&
      getOptionSecondaryParentId
    ) {
      const parent = getOptionSecondaryParentId(option);
      if (parent === null || parent === undefined) {
        return false;
      }
      if (!secondaryParentIds.includes(parent)) {
        return false;
      }
    }
    return true;
  });

  const selectedSet = new Set<Id>(selected);
  const selectedOptions = filteredOptions.filter((option) =>
    selectedSet.has(getOptionId(option)),
  );

  return (
    <Autocomplete
      multiple
      size={size}
      options={filteredOptions}
      value={selectedOptions}
      onChange={(_, values) => onChange(values.map(getOptionId))}
      disableCloseOnSelect
      groupBy={groupBy}
      isOptionEqualToValue={(option, value) => getOptionId(option) === getOptionId(value)}
      getOptionLabel={getOptionLabel}
      renderTags={() => null}
      renderGroup={
        groupBy
          ? (params: AutocompleteRenderGroupParams) => (
              <li key={params.key}>
                <Box
                  sx={{
                    position: "sticky",
                    top: -8,
                    zIndex: 1,
                    px: 1.5,
                    py: 0.5,
                    bgcolor: "#f0f2f3",
                    borderBottom: "1px solid rgba(17, 61, 73, 0.1)",
                  }}
                >
                  <Typography
                    variant="caption"
                    sx={{
                      fontWeight: 700,
                      color: "#47626b",
                      textTransform: "uppercase",
                      fontSize: "0.68rem",
                      letterSpacing: 0.4,
                    }}
                  >
                    {params.group}
                  </Typography>
                </Box>
                <ul style={{ padding: 0 }}>{params.children}</ul>
              </li>
            )
          : undefined
      }
      renderOption={(props, option, { selected: isSelected }) => (
        <li
          {...props}
          key={String(getOptionId(option))}
          style={{ fontSize: "0.78rem", paddingTop: 2, paddingBottom: 2 }}
        >
          <Checkbox
            icon={<CheckBoxOutlineBlankIcon fontSize="small" />}
            checkedIcon={<CheckBoxIcon fontSize="small" />}
            style={{ marginRight: 6, padding: 2 }}
            checked={isSelected}
          />
          {getOptionLabel(option)}
        </li>
      )}
      ListboxProps={
        pseudoOption
          ? {
              // Wrap the default listbox contents with the pseudo-option at top.
              // This is a lightweight hack: we render the pseudo row as a sibling
              // above the normal options via a custom PaperComponent would be
              // cleaner but this keeps the API tight without a second wrapper.
              ...{},
            }
          : undefined
      }
      PaperComponent={
        pseudoOption
          ? (paperProps) => (
              <PseudoOptionPaper pseudo={pseudoOption} {...paperProps} />
            )
          : undefined
      }
      renderInput={(params) => (
        <TextField
          {...params}
          label={label}
          placeholder={placeholder}
          helperText={helperText}
          inputProps={{ ...params.inputProps, sx: { fontSize: "0.78rem" } }}
          InputLabelProps={{ sx: { fontSize: "0.78rem" } }}
          FormHelperTextProps={{
            sx: { fontSize: "0.65rem", mt: 0.25, lineHeight: 1.3 },
          }}
        />
      )}
    />
  );
}

// Internal: the Paper wrapper that injects the pseudo-option row at the top
// of the dropdown. Kept in the same file so the GeographyAutocomplete API
// remains self-contained.
function PseudoOptionPaper({
  pseudo,
  children,
  ...paperProps
}: {
  pseudo: PseudoOptionSpec;
  children?: React.ReactNode;
} & React.HTMLAttributes<HTMLDivElement>) {
  return (
    <Box
      {...paperProps}
      sx={{
        bgcolor: "background.paper",
        boxShadow: 4,
        borderRadius: 1,
      }}
    >
      <Box
        role="option"
        aria-selected={pseudo.selected}
        onMouseDown={(event) => {
          // Prevent the autocomplete input from losing focus / the dropdown
          // from closing before our toggle runs.
          event.preventDefault();
        }}
        onClick={() => pseudo.onToggle(!pseudo.selected)}
        sx={{
          px: 1.5,
          py: 1,
          display: "flex",
          alignItems: "center",
          gap: 0.5,
          borderBottom: "1px solid rgba(17, 61, 73, 0.12)",
          cursor: "pointer",
          "&:hover": { bgcolor: "rgba(17, 61, 73, 0.04)" },
        }}
      >
        <Checkbox
          icon={<CheckBoxOutlineBlankIcon fontSize="small" />}
          checkedIcon={<CheckBoxIcon fontSize="small" />}
          size="small"
          style={{ marginRight: 6, padding: 2 }}
          checked={pseudo.selected}
          // Let the parent Box onClick drive the toggle; stop the checkbox
          // from double-firing.
          onClick={(event) => event.stopPropagation()}
          onChange={(_, checked) => pseudo.onToggle(checked)}
        />
        <Box sx={{ fontSize: "0.78rem", fontStyle: "italic", flex: 1 }}>
          {pseudo.label}
        </Box>
      </Box>
      {pseudo.helperText && (
        <Box
          sx={{
            px: 1.5,
            py: 0.5,
            fontSize: "0.65rem",
            color: "#47626b",
            borderBottom: "1px solid rgba(17, 61, 73, 0.06)",
          }}
        >
          {pseudo.helperText}
        </Box>
      )}
      {children}
    </Box>
  );
}
