from __future__ import annotations

import os
from collections.abc import Sequence

import pandas as pd
from sqlalchemy import create_engine


def get_database_url(libref: str, engine_name: str) -> str:
    env_name = f"SAS_MIGRATOR_DB_{libref.upper()}_URL"
    url = os.getenv(env_name)
    if not url:
        raise RuntimeError(
            f"Missing database URL for SAS libref {libref!r}. "
            f"Set {env_name} to a SQLAlchemy URL for the {engine_name} connection."
        )
    return url


def read_database_table(libref: str, engine_name: str, table: str, where_sql: str | None = None) -> pd.DataFrame:
    engine = create_engine(get_database_url(libref, engine_name))
    if where_sql:
        return pd.read_sql(f"SELECT * FROM {table} WHERE {where_sql}", con=engine)
    return pd.read_sql_table(table, con=engine)


def sas_sort(df: pd.DataFrame, by: Sequence[str], ascending: Sequence[bool] | None = None) -> pd.DataFrame:
    ascending = list(ascending) if ascending is not None else [True] * len(by)
    work = df.copy()
    work["_sas_row_order"] = range(len(work))
    sorted_df = work.sort_values(
        by=list(by) + ["_sas_row_order"],
        ascending=list(ascending) + [True],
        kind="mergesort",
        na_position="first",
    )
    return sorted_df.drop(columns=["_sas_row_order"]).reset_index(drop=True)


def sas_first_last_flags(df: pd.DataFrame, by: Sequence[str]) -> pd.DataFrame:
    if not by:
        return df.copy()
    result = sas_sort(df, by)
    group_cols = list(by)
    for idx, col in enumerate(group_cols):
        grouped = result.groupby(group_cols[: idx + 1], sort=False, dropna=False)
        result[f"FIRST_{col}"] = grouped.cumcount().eq(0)
        sizes = grouped[group_cols[0]].transform("size")
        result[f"LAST_{col}"] = grouped.cumcount().eq(sizes - 1)
    return result


def sas_style_merge(
    inputs: Sequence[tuple[str, pd.DataFrame]],
    by: Sequence[str],
    keep_flags: Sequence[str] | None = None,
) -> pd.DataFrame:
    if not inputs:
        return pd.DataFrame()
    by_cols = list(by)
    keep_flags = {flag.lower() for flag in keep_flags or []}
    flag, first_df = inputs[0]
    result = sas_sort(first_df, by_cols)
    result[f"_in_{flag.lower()}"] = True
    for right_flag, right_df in inputs[1:]:
        right_flag = right_flag.lower()
        right_sorted = sas_sort(right_df, by_cols)
        result = result.merge(
            right_sorted,
            on=by_cols,
            how="outer",
            suffixes=("", f"_{right_flag}"),
            indicator=f"_merge_{right_flag}",
        )
        result[f"_in_{right_flag}"] = result[f"_merge_{right_flag}"].ne("left_only")
        result = result.drop(columns=[f"_merge_{right_flag}"])
    if keep_flags:
        mask = pd.Series(True, index=result.index)
        for flag in keep_flags:
            mask = mask & result.get(f"_in_{flag}", False)
        result = result.loc[mask].copy()
    in_cols = [col for col in result.columns if col.startswith("_in_")]
    return result.drop(columns=in_cols).reset_index(drop=True)


def sas_retain_cumcount(df: pd.DataFrame, by: Sequence[str], target: str, start: int = 1) -> pd.DataFrame:
    result = sas_first_last_flags(df, by)
    group_cols = list(by)
    if group_cols:
        result[target] = result.groupby(group_cols, sort=False, dropna=False).cumcount() + start
    else:
        result[target] = range(start, start + len(result))
    return result


def sas_date_literal(value: str) -> pd.Timestamp:
    return pd.to_datetime(value.strip().strip("'").removesuffix("D"), format="%d%b%Y")


def sas_year(value):
    return pd.to_datetime(value).dt.year if hasattr(value, "dt") or isinstance(value, pd.Series) else pd.to_datetime(value).year


def sas_month(value):
    return pd.to_datetime(value).dt.month if hasattr(value, "dt") or isinstance(value, pd.Series) else pd.to_datetime(value).month


def sas_day(value):
    return pd.to_datetime(value).dt.day if hasattr(value, "dt") or isinstance(value, pd.Series) else pd.to_datetime(value).day


def sas_days_between(left, right):
    delta = pd.to_datetime(left) - pd.to_datetime(right)
    return delta.dt.days if hasattr(delta, "dt") else delta.days
