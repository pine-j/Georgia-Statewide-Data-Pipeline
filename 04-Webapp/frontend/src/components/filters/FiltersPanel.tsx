import RestartAltRoundedIcon from "@mui/icons-material/RestartAltRounded";
import CloseRoundedIcon from "@mui/icons-material/CloseRounded";
import {
  Autocomplete,
  Box,
  Button,
  IconButton,
  Paper,
  Stack,
  TextField,
  Typography,
} from "@mui/material";

import { CountyOption, DistrictOption } from "../../types/api";

function displayDistrictLabel(label: string): string {
  const separatorIndex = label.indexOf(" - ");
  return separatorIndex >= 0 ? label.slice(separatorIndex + 3) : label;
}

interface FiltersPanelProps {
  districts: DistrictOption[];
  counties: CountyOption[];
  selectedDistrict: number | null;
  selectedCounties: string[];
  onDistrictChange: (district: number | null) => void;
  onCountyChange: (counties: string[]) => void;
  onCountyDelete: (county: string) => void;
  onResetFilters: () => void;
}

export function FiltersPanel({
  districts,
  counties,
  selectedDistrict,
  selectedCounties,
  onDistrictChange,
  onCountyChange,
  onCountyDelete,
  onResetFilters,
}: FiltersPanelProps) {
  const selectedDistrictOption = districts.find((district) => district.id === selectedDistrict);
  const countyOptions = selectedDistrict
    ? counties.filter((county) => county.district === selectedDistrict)
    : counties;
  const hasActiveFilters = Boolean(selectedDistrict || selectedCounties.length);

  return (
    <Paper
      elevation={0}
      sx={{
        p: 2,
        borderRadius: 0,
        border: "0",
        bgcolor: "#ffffff",
        height: "100%",
        overflowY: "auto",
      }}
    >
      <Stack spacing={2}>
        <Stack
          direction="row"
          justifyContent="space-between"
          alignItems="center"
          spacing={1}
        >
          <Typography variant="subtitle1" sx={{ fontWeight: 700 }}>
            Filters
          </Typography>
        </Stack>

        {hasActiveFilters && (
          <Stack spacing={0.5} sx={{ pt: 0.25 }}>
            <Box
              sx={{
                display: "flex",
                alignItems: "center",
                justifyContent: "space-between",
                gap: 1,
              }}
            >
              <Typography variant="caption" color="text.secondary" sx={{ letterSpacing: 0.3 }}>
                Active Filters
              </Typography>
              <Button
                size="small"
                color="inherit"
                startIcon={<RestartAltRoundedIcon fontSize="small" />}
                onClick={onResetFilters}
                sx={{ minWidth: 0, px: 0.5, py: 0, fontSize: "0.7rem", lineHeight: 1.2 }}
              >
                Reset all
              </Button>
            </Box>

            {selectedDistrictOption && (
              <Box
                sx={{
                  display: "flex",
                  alignItems: "center",
                  gap: 0.5,
                  minWidth: 0,
                  lineHeight: 1.2,
                }}
              >
                <Typography variant="caption" color="text.secondary" sx={{ flexShrink: 0 }}>
                  District:
                </Typography>
                <Box
                  sx={{
                    display: "inline-flex",
                    alignItems: "center",
                    gap: 0.25,
                    minWidth: 0,
                    px: 0.5,
                    py: 0.125,
                    borderRadius: "4px",
                    bgcolor: "rgba(17, 61, 73, 0.06)",
                  }}
                >
                  <Typography
                    variant="caption"
                    sx={{
                      color: "text.primary",
                      fontWeight: 600,
                      minWidth: 0,
                      overflow: "hidden",
                      textOverflow: "ellipsis",
                      whiteSpace: "nowrap",
                    }}
                  >
                    {displayDistrictLabel(selectedDistrictOption.label)}
                  </Typography>
                  <IconButton
                    size="small"
                    onClick={() => onDistrictChange(null)}
                    aria-label="Remove district filter"
                    sx={{ p: 0.25 }}
                  >
                    <CloseRoundedIcon fontSize="inherit" />
                  </IconButton>
                </Box>
              </Box>
            )}

            {selectedCounties.length > 0 && (
              <Box
                sx={{
                  display: "flex",
                  alignItems: "flex-start",
                  gap: 0.5,
                  minWidth: 0,
                }}
              >
                <Typography variant="caption" color="text.secondary" sx={{ flexShrink: 0 }}>
                  County:
                </Typography>
                <Box
                  sx={{
                    minWidth: 0,
                    flex: 1,
                    display: "flex",
                    flexWrap: "wrap",
                    gap: 0.5,
                  }}
                >
                  {selectedCounties.map((county) => (
                    <Box
                      key={county}
                      sx={{
                        display: "flex",
                        alignItems: "center",
                        gap: 0.5,
                        minWidth: 0,
                        px: 0.5,
                        py: 0.125,
                        borderRadius: "4px",
                        bgcolor: "rgba(17, 61, 73, 0.06)",
                      }}
                    >
                      <Typography
                        variant="caption"
                        sx={{
                          color: "text.primary",
                          fontWeight: 600,
                          minWidth: 0,
                          overflow: "hidden",
                          textOverflow: "ellipsis",
                          whiteSpace: "nowrap",
                        }}
                      >
                        {county}
                      </Typography>
                      <IconButton
                        size="small"
                        onClick={() => onCountyDelete(county)}
                        aria-label={`Remove ${county} county filter`}
                        sx={{ p: 0.25 }}
                      >
                        <CloseRoundedIcon fontSize="inherit" />
                      </IconButton>
                    </Box>
                  ))}
                </Box>
              </Box>
            )}
          </Stack>
        )}

        <TextField
          select
          label="District"
          size="small"
          value={selectedDistrict ?? ""}
          onChange={(event) => {
            const value = event.target.value;
            onDistrictChange(value === "" ? null : Number(value));
          }}
          SelectProps={{ native: true }}
          fullWidth
        >
          <option value="">All districts</option>
          {districts.map((district) => (
            <option key={district.id} value={district.id}>
              {displayDistrictLabel(district.label)}
            </option>
          ))}
        </TextField>

        <Autocomplete
          multiple
          size="small"
          options={countyOptions.map((county) => county.county)}
          value={selectedCounties}
          onChange={(_, values) => onCountyChange(values)}
          disableCloseOnSelect
          filterSelectedOptions
          renderTags={() => null}
          renderInput={(params) => (
            <TextField
              {...params}
              label="County"
              placeholder="Search counties"
            />
          )}
        />
      </Stack>
    </Paper>
  );
}
