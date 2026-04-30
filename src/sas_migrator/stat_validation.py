from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class MetricDifference:
    metric: str
    sas_value: float
    python_value: float
    difference: float
    within_tolerance: bool


def compare_statistical_metrics(
    sas_results: dict[str, float],
    python_results: dict[str, float],
    tolerance: float = 1e-10,
) -> list[MetricDifference]:
    differences = []
    for metric, sas_value in sas_results.items():
        if metric not in python_results:
            continue
        python_value = python_results[metric]
        diff = abs(float(sas_value) - float(python_value))
        differences.append(
            MetricDifference(
                metric=metric,
                sas_value=float(sas_value),
                python_value=float(python_value),
                difference=diff,
                within_tolerance=diff <= tolerance,
            )
        )
    return differences


def listwise_delete_for_sas(df: pd.DataFrame, columns: list[str] | None = None) -> pd.DataFrame:
    if columns:
        return df.dropna(subset=columns).copy()
    return df.dropna().copy()


def sas_anova_type() -> int:
    return 2
