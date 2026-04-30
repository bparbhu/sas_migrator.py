import pandas as pd
from sas_migrator.runtime import (
    read_database_table,
    sas_date_literal,
    sas_day,
    sas_first_last_flags,
    sas_month,
    sas_retain_cumcount,
    sas_sort,
    sas_style_merge,
    sas_year,
)

work_daily = staging_orders.loc[:, ['id', 'qty', 'price', 'status']].copy()
work_daily = work_daily.loc[work_daily["status"] == 'OPEN'].copy()
work_daily["total"] = work_daily["qty"] * work_daily["price"]

work_daily_summary = (
    work_daily.groupby(['status'], as_index=False)
    .agg(
        total_mean=("total", "mean"),
        total_sum=("total", "sum"),
    )
)
