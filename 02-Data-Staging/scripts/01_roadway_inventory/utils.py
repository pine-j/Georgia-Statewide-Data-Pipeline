"""Shared helper functions for the roadway inventory pipeline."""

from __future__ import annotations

from typing import Any

import pandas as pd

MILEPOINT_TOLERANCE = 1e-4


def decode_lookup_value(value: Any, lookup: dict, zero_pad: int | None = None) -> str | None:
    if pd.isna(value):
        return None

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text in lookup:
            return lookup[text]
        upper_text = text.upper()
        if upper_text in lookup:
            return lookup[upper_text]
        try:
            numeric_text = int(float(text))
        except ValueError:
            return None
        if zero_pad is not None:
            padded = f"{numeric_text:0{zero_pad}d}"
            if padded in lookup:
                return lookup[padded]
        return lookup.get(numeric_text) or lookup.get(str(numeric_text))

    try:
        numeric_value = int(float(value))
    except (TypeError, ValueError):
        return None

    if zero_pad is not None:
        padded = f"{numeric_value:0{zero_pad}d}"
        if padded in lookup:
            return lookup[padded]

    return lookup.get(numeric_value) or lookup.get(str(numeric_value))


def clean_text(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def round_milepoint(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    rounded = round(float(value), 4)
    return 0.0 if abs(rounded) < MILEPOINT_TOLERANCE else rounded


# Backward-compatible aliases while modules migrate to the public names.
_clean_text = clean_text
_round_milepoint = round_milepoint
