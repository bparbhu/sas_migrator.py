from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class SasColumnProfile:
    name: str
    dtype: str
    missing_count: int
    missing_percent: float
    sas_missing_like_values: list[str] = field(default_factory=list)
    label: str | None = None
    format: str | None = None


@dataclass(frozen=True)
class SasDataProfile:
    source: str
    row_count: int
    column_count: int
    columns: list[SasColumnProfile]
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "source": self.source,
            "row_count": self.row_count,
            "column_count": self.column_count,
            "columns": [asdict(col) for col in self.columns],
            "metadata": self.metadata,
        }


def sas_missing_like_values(series: pd.Series) -> list[str]:
    values = []
    if series.dtype == "object" or str(series.dtype).startswith("string"):
        for value in series.dropna().astype(str).unique():
            stripped = value.strip()
            if stripped == "." or stripped.startswith("."):
                values.append(value)
    return sorted(values)


def normalize_sas_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    for col in result.columns:
        if result[col].dtype == "object" or str(result[col].dtype).startswith("string"):
            mask = result[col].astype("string").str.strip().str.startswith(".", na=False)
            result.loc[mask, col] = pd.NA
    return result


def optimize_dataframe_types(
    df: pd.DataFrame,
    categorical_threshold: float = 0.10,
    preserve_columns: set[str] | None = None,
) -> pd.DataFrame:
    preserve_columns = preserve_columns or set()
    result = df.copy()
    for col in result.columns:
        if col in preserve_columns:
            continue
        series = result[col]
        if pd.api.types.is_integer_dtype(series):
            result[col] = pd.to_numeric(series, downcast="integer")
        elif pd.api.types.is_float_dtype(series):
            result[col] = pd.to_numeric(series, downcast="float")
        elif series.dtype == "object" or str(series.dtype).startswith("string"):
            unique_ratio = series.nunique(dropna=True) / max(len(series), 1)
            if unique_ratio <= categorical_threshold:
                result[col] = series.astype("category")
            else:
                result[col] = series.astype("string")
    return result


def profile_dataframe(
    df: pd.DataFrame,
    source: str,
    labels: dict[str, str] | None = None,
    formats: dict[str, str] | None = None,
    metadata: dict[str, Any] | None = None,
) -> SasDataProfile:
    labels = labels or {}
    formats = formats or {}
    columns = []
    row_count = len(df)
    for col in df.columns:
        missing_count = int(df[col].isna().sum())
        columns.append(
            SasColumnProfile(
                name=col,
                dtype=str(df[col].dtype),
                missing_count=missing_count,
                missing_percent=missing_count / row_count if row_count else 0.0,
                sas_missing_like_values=sas_missing_like_values(df[col]),
                label=labels.get(col),
                format=formats.get(col),
            )
        )
    return SasDataProfile(
        source=source,
        row_count=row_count,
        column_count=len(df.columns),
        columns=columns,
        metadata=metadata or {},
    )


def read_sas_dataset(path: Path, normalize_missing: bool = True) -> tuple[pd.DataFrame, dict[str, Any]]:
    metadata: dict[str, Any] = {"reader": "pandas.read_sas"}
    try:
        import pyreadstat  # type: ignore
    except ImportError:
        df = pd.read_sas(path, format="sas7bdat" if path.suffix.lower() == ".sas7bdat" else None)
        return (normalize_sas_missing_values(df) if normalize_missing else df), metadata

    df, meta = pyreadstat.read_sas7bdat(path) if path.suffix.lower() == ".sas7bdat" else pyreadstat.read_xport(path)
    metadata = {
        "reader": "pyreadstat",
        "column_labels": dict(zip(meta.column_names, meta.column_labels or [])),
        "column_formats": getattr(meta, "original_variable_types", {}) or {},
        "file_label": getattr(meta, "file_label", None),
    }
    return (normalize_sas_missing_values(df) if normalize_missing else df), metadata


def convert_sas_file_to_parquet(
    source_path: Path,
    output_path: Path,
    profile_path: Path | None = None,
    compression: str | None = "snappy",
    optimize_types: bool = True,
) -> SasDataProfile:
    df, metadata = read_sas_dataset(source_path)
    if optimize_types:
        df = optimize_dataframe_types(df)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, compression=compression, engine="pyarrow")
    labels = metadata.get("column_labels") or {}
    formats = metadata.get("column_formats") or {}
    profile = profile_dataframe(df, str(source_path), labels=labels, formats=formats, metadata=metadata)
    if profile_path:
        profile_path.parent.mkdir(parents=True, exist_ok=True)
        profile_path.write_text(json.dumps(profile.to_dict(), indent=2), encoding="utf-8")
    return profile


def convert_sas_folder_to_parquet(source_root: Path, output_root: Path) -> dict:
    converted = []
    failed = []
    for path in sorted(source_root.rglob("*")):
        if path.suffix.lower() not in {".sas7bdat", ".xpt"}:
            continue
        rel = path.relative_to(source_root)
        parquet_path = output_root / rel.with_suffix(".parquet")
        profile_path = output_root / rel.with_suffix(".profile.json")
        try:
            profile = convert_sas_file_to_parquet(path, parquet_path, profile_path)
            converted.append({"source": str(rel), "rows": profile.row_count, "columns": profile.column_count})
        except Exception as exc:
            failed.append({"source": str(rel), "error": str(exc)})
    summary = {"converted_count": len(converted), "failed_count": len(failed), "converted": converted, "failed": failed}
    output_root.mkdir(parents=True, exist_ok=True)
    (output_root / "sas_data_ingest_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary
