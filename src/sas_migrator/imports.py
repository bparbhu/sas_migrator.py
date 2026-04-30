from __future__ import annotations

import re


PANDAS_RUNTIME_HELPERS = [
    "read_database_table",
    "sas_date_literal",
    "sas_day",
    "sas_first_last_flags",
    "sas_month",
    "sas_retain_cumcount",
    "sas_sort",
    "sas_style_merge",
    "sas_year",
]


def helper_is_used(code: str, helper: str) -> bool:
    return re.search(rf"\b{re.escape(helper)}\s*\(", code) is not None


def build_pandas_imports(body_lines: list[str]) -> list[str]:
    body = "\n".join(body_lines)
    imports = ["import pandas as pd"]
    used_helpers = [helper for helper in PANDAS_RUNTIME_HELPERS if helper_is_used(body, helper)]
    if used_helpers:
        imports.append("from sas_migrator.runtime import (")
        imports.extend(f"    {helper}," for helper in used_helpers)
        imports.append(")")
    return imports
