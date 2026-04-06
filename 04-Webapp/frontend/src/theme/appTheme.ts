import { createTheme } from "@mui/material/styles";

export const appTheme = createTheme({
  palette: {
    mode: "light",
    primary: {
      main: "#0b5c6b",
    },
    secondary: {
      main: "#a95f2a",
    },
    background: {
      default: "#f6fafb",
      paper: "#ffffff",
    },
  },
  shape: {
    borderRadius: 18,
  },
  typography: {
    fontFamily: '"Trebuchet MS", "Segoe UI", sans-serif',
    h1: {
      fontSize: "2rem",
      fontWeight: 700,
    },
    h5: {
      fontWeight: 700,
    },
  },
});

