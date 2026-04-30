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

work_customer_rollup_filtered = sales_local.loc[sales_local["region"] == 'East'].copy()
work_customer_rollup = (
    work_customer_rollup_filtered.groupby(['customer_id'], as_index=False)
    .agg(
        total_amount=("amount", "sum"),
        max_amount=("amount", "max"),
    )
)
work_customer_rollup = work_customer_rollup.sort_values(by=['customer_id'], ascending=[True]).reset_index(drop=True)
