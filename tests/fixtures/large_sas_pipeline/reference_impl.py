from __future__ import annotations

import pandas as pd

from sas_migrator.runtime import sas_first_last_flags, sas_sort, sas_style_merge


def input_tables() -> dict[str, pd.DataFrame]:
    return {
        "raw_orders": pd.DataFrame(
            [
                {"customer_id": 1, "order_id": 101, "order_date": "2024-01-01", "amount": 10.0, "qty": 1, "price": 10.0, "status": "OPEN", "region": "East"},
                {"customer_id": 1, "order_id": 102, "order_date": "2024-01-05", "amount": 20.0, "qty": 2, "price": 10.0, "status": "OPEN", "region": "East"},
                {"customer_id": 1, "order_id": 103, "order_date": "2024-01-07", "amount": 15.0, "qty": 3, "price": 5.0, "status": "CLOSED", "region": "East"},
                {"customer_id": 2, "order_id": 201, "order_date": "2024-01-03", "amount": 12.0, "qty": 2, "price": 6.0, "status": "OPEN", "region": "West"},
                {"customer_id": 2, "order_id": 202, "order_date": "2024-01-04", "amount": 24.0, "qty": 4, "price": 6.0, "status": "OPEN", "region": "North"},
                {"customer_id": 3, "order_id": 301, "order_date": "2024-01-02", "amount": 30.0, "qty": 5, "price": 6.0, "status": "OPEN", "region": "South"},
                {"customer_id": 4, "order_id": 401, "order_date": "2024-01-08", "amount": 8.0, "qty": 1, "price": 8.0, "status": "OPEN", "region": "West"},
            ]
        ),
        "work_customers": pd.DataFrame(
            [
                {"customer_id": 1, "segment": "A"},
                {"customer_id": 2, "segment": "B"},
                {"customer_id": 3, "segment": "A"},
                {"customer_id": 5, "segment": "C"},
            ]
        ),
    }


def expected_outputs() -> dict[str, pd.DataFrame]:
    tables = input_tables()
    raw_orders = tables["raw_orders"]
    work_customers = tables["work_customers"]

    work_orders_open = raw_orders.loc[:, ["customer_id", "order_id", "order_date", "amount", "qty", "price", "status", "region"]].copy()
    work_orders_open = work_orders_open.loc[work_orders_open["status"] == "OPEN"].copy()
    work_orders_open["revenue"] = work_orders_open["qty"] * work_orders_open["price"]

    work_orders_sorted = sas_sort(work_orders_open, ["customer_id", "order_date"])

    work_last_order = sas_first_last_flags(work_orders_sorted, ["customer_id", "order_date"])
    work_last_order = work_last_order.loc[work_last_order["LAST_customer_id"]].copy()

    work_customer_rollup_filtered = work_orders_open.loc[work_orders_open["region"] != "West"].copy()
    work_customer_rollup = (
        work_customer_rollup_filtered.groupby(["customer_id"], as_index=False)
        .agg(total_revenue=("revenue", "sum"), max_amount=("amount", "max"))
        .sort_values(["customer_id"], kind="mergesort")
        .reset_index(drop=True)
    )

    work_customer_region_freq = (
        work_orders_open.groupby(["customer_id", "region"], as_index=False)
        .size()
        .rename(columns={"size": "count"})
    )

    work_region_summary = (
        work_orders_open.groupby(["region"], as_index=False)
        .agg(revenue_mean=("revenue", "mean"), revenue_sum=("revenue", "sum"))
    )

    work_customer_final = sas_style_merge(
        [("a", work_customer_rollup), ("b", work_customers)],
        by=["customer_id"],
        keep_flags=["a", "b"],
    )

    work_order_sequence = sas_first_last_flags(work_orders_sorted, ["customer_id", "order_date"])
    work_order_sequence["visit_count"] = work_order_sequence.groupby(["customer_id"], sort=False, dropna=False).cumcount() + 1
    work_order_sequence["order_sequence"] = work_order_sequence["visit_count"]

    work_orders_imputed = work_orders_open.copy()
    work_orders_imputed["amount"] = work_orders_imputed["amount"].fillna(work_orders_imputed["amount"].median())
    work_orders_imputed["revenue"] = work_orders_imputed["revenue"].fillna(work_orders_imputed["revenue"].median())

    return {
        "work_orders_open": work_orders_open,
        "work_orders_sorted": work_orders_sorted,
        "work_last_order": work_last_order,
        "work_customer_rollup": work_customer_rollup,
        "work_customer_region_freq": work_customer_region_freq,
        "work_region_summary": work_region_summary,
        "work_customer_final": work_customer_final,
        "work_order_sequence": work_order_sequence,
        "work_orders_imputed": work_orders_imputed,
    }


def sort_keys() -> dict[str, list[str]]:
    return {
        "work_orders_open": ["customer_id", "order_id"],
        "work_orders_sorted": ["customer_id", "order_date", "order_id"],
        "work_last_order": ["customer_id"],
        "work_customer_rollup": ["customer_id"],
        "work_customer_region_freq": ["customer_id", "region"],
        "work_region_summary": ["region"],
        "work_customer_final": ["customer_id"],
        "work_order_sequence": ["customer_id", "order_date", "order_id"],
        "work_orders_imputed": ["customer_id", "order_id"],
    }
