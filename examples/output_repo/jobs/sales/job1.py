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

dw_sales = read_database_table("dw", "oracle", "sales", None)
sales_local = dw_sales.loc[:, ['customer_id', 'order_date', 'amount', 'region']].copy()
sales_local["revenue"] = sales_local["amount"]

sales_sorted = sales_local.sort_values(by=['customer_id', 'order_date'], ascending=[True, False]).reset_index(drop=True)

sales_freq = (sales_sorted.groupby(['customer_id', 'region'], as_index=False).size().rename(columns={'size': 'count'}))
