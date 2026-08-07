"""Privacy-aware CSV profiling for wide and long source layouts."""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import polars as pl

from .errors import V2Error


def _finite(value: float | None) -> float | None:
    return value if value is not None and math.isfinite(value) else None


def _column_profile(series: pl.Series, force_frequencies: bool = False) -> dict[str, Any]:
    row_count = len(series)
    non_null = series.drop_nulls()
    null_count = row_count - len(non_null)
    unique_count = non_null.n_unique()
    strings = non_null.cast(pl.String)
    numeric = strings.cast(pl.Float64, strict=False)
    numeric_count = numeric.drop_nulls().len()
    non_numeric_count = len(non_null) - numeric_count
    inferred = "numeric" if len(non_null) and non_numeric_count == 0 else "string"
    result: dict[str, Any] = {
        "rowCount": row_count,
        "nullCount": null_count,
        "uniqueCount": unique_count,
        "uniqueRatio": unique_count / row_count if row_count else 0,
        "inferredType": inferred,
        "nonNumericCount": non_numeric_count,
    }
    if numeric_count:
        result["numeric"] = {
            "minimum": _finite(numeric.min()),
            "maximum": _finite(numeric.max()),
            "mean": _finite(numeric.mean()),
            "median": _finite(numeric.median()),
        }
    if force_frequencies or not row_count or unique_count / row_count < 0.7:
        counts = strings.value_counts(sort=True).head(500)
        result["valueFrequencies"] = [
            {"value": row[0], "count": row[1]} for row in counts.iter_rows()
        ]
        result["frequenciesTruncated"] = unique_count > 500
    else:
        result["frequenciesSuppressed"] = True
    return result


def read_source(path: str | Path) -> pl.DataFrame:
    try:
        return pl.read_csv(
            path,
            infer_schema=False,
            null_values=["", "NA", "N/A", "null", "NULL"],
            truncate_ragged_lines=False,
        )
    except Exception as exc:
        raise V2Error("invalid_csv", "The CSV file could not be parsed", 422, details={"reason": str(exc)}) from exc


def profile_csv(path: str | Path, layout: str, roles: dict[str, str]) -> dict[str, Any]:
    frame = read_source(path)
    if not frame.columns:
        raise V2Error("empty_csv", "The CSV file contains no columns", 422)
    unknown = sorted({value for value in roles.values() if value and value not in frame.columns})
    if unknown:
        raise V2Error("invalid_roles", "Column roles reference missing columns", 422, details={"columns": unknown})
    columns = {
        name: _column_profile(frame[name], name == roles.get("eventType"))
        for name in frame.columns
    }
    result: dict[str, Any] = {
        "layout": layout,
        "roles": roles,
        "rowCount": frame.height,
        "columnCount": frame.width,
        "columns": columns,
    }
    if layout == "long":
        discriminator = roles.get("eventType")
        value_column = roles.get("eventValue")
        if not discriminator or not value_column:
            raise V2Error("invalid_roles", "Long data requires eventType and eventValue roles", 422)
        conditional: dict[str, Any] = {}
        for event_value in frame[discriminator].drop_nulls().unique().sort().to_list()[:500]:
            subset = frame.filter(pl.col(discriminator) == event_value)
            conditional[str(event_value)] = _column_profile(subset[value_column])
        result["conditionalValueProfiles"] = conditional
    return result
