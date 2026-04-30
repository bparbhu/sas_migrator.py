from __future__ import annotations

import pandas as pd

from sas_migrator.data_io import (
    normalize_sas_missing_values,
    optimize_dataframe_types,
    profile_dataframe,
    sas_missing_like_values,
)


def test_sas_missing_like_values_detects_special_missing_strings():
    ser = pd.Series([".", ".A", ".Z", "real", None])
    assert sas_missing_like_values(ser) == [".", ".A", ".Z"]


def test_normalize_sas_missing_values_converts_dot_values_to_na():
    df = pd.DataFrame({"code": [".", ".A", "OK"], "value": [1, 2, 3]})
    normalized = normalize_sas_missing_values(df)
    assert normalized["code"].isna().tolist() == [True, True, False]
    assert normalized["value"].tolist() == [1, 2, 3]


def test_profile_dataframe_includes_labels_formats_and_missing_counts():
    df = pd.DataFrame({"id": [1, 2], "code": ["A", pd.NA]})
    profile = profile_dataframe(
        df,
        "example.sas7bdat",
        labels={"code": "Status code"},
        formats={"code": "$CHAR."},
        metadata={"reader": "test"},
    )
    assert profile.row_count == 2
    assert profile.column_count == 2
    code_profile = [col for col in profile.columns if col.name == "code"][0]
    assert code_profile.label == "Status code"
    assert code_profile.format == "$CHAR."
    assert code_profile.missing_count == 1


def test_optimize_dataframe_types_converts_low_cardinality_strings_to_category():
    df = pd.DataFrame({"group": ["A", "A", "B", "B"] * 10, "value": [1.0, 2.0, 3.0, 4.0] * 10})
    optimized = optimize_dataframe_types(df, categorical_threshold=0.20)
    assert str(optimized["group"].dtype) == "category"
    assert str(optimized["value"].dtype) == "float32"
