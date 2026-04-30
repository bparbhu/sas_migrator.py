from __future__ import annotations

import importlib.util
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class DataFrameComparison:
    name: str
    passed: bool
    row_count_left: int
    row_count_right: int
    columns_left: list[str]
    columns_right: list[str]
    message: str


def normalize_dataframe(df: pd.DataFrame, sort_by: list[str] | None = None) -> pd.DataFrame:
    normalized = df.copy()
    if sort_by:
        present = [col for col in sort_by if col in normalized.columns]
        if present:
            normalized = normalized.sort_values(present, kind="mergesort")
    normalized = normalized.reset_index(drop=True)
    return normalized


def compare_dataframes(
    name: str,
    left: pd.DataFrame,
    right: pd.DataFrame,
    sort_by: list[str] | None = None,
    rtol: float = 1e-9,
    atol: float = 1e-12,
) -> DataFrameComparison:
    left_norm = normalize_dataframe(left, sort_by)
    right_norm = normalize_dataframe(right, sort_by)
    try:
        pd.testing.assert_frame_equal(
            left_norm,
            right_norm,
            check_dtype=False,
            check_like=True,
            rtol=rtol,
            atol=atol,
        )
    except AssertionError as exc:
        return DataFrameComparison(
            name=name,
            passed=False,
            row_count_left=len(left_norm),
            row_count_right=len(right_norm),
            columns_left=list(left_norm.columns),
            columns_right=list(right_norm.columns),
            message=str(exc),
        )
    return DataFrameComparison(
        name=name,
        passed=True,
        row_count_left=len(left_norm),
        row_count_right=len(right_norm),
        columns_left=list(left_norm.columns),
        columns_right=list(right_norm.columns),
        message="matched",
    )


def execute_python_file(path: Path, namespace: dict[str, Any]) -> dict[str, Any]:
    code = path.read_text(encoding="utf-8")
    compiled = compile(code, str(path), "exec")
    exec(compiled, namespace)
    return namespace


def load_reference_module(path: Path):
    spec = importlib.util.spec_from_file_location(path.stem, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load reference module from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def write_comparison_report(comparisons: list[DataFrameComparison], output_path: Path) -> None:
    import json

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(
            {
                "passed": all(item.passed for item in comparisons),
                "comparisons": [asdict(item) for item in comparisons],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
