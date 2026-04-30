from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class SparkPattern:
    sas_pattern: str
    pyspark_target: str
    notes: str


SPARK_PATTERNS: list[SparkPattern] = [
    SparkPattern("DATA step assignment", "withColumn", "Create or replace columns with expressions."),
    SparkPattern("KEEP dataset option", "select", "Select output columns."),
    SparkPattern("DROP dataset option", "drop", "Drop output columns."),
    SparkPattern("WHERE / subsetting IF", "filter / where", "Filter rows with Column expressions."),
    SparkPattern("PROC SORT BY", "orderBy / sort", "Sort by one or more columns; specify asc/desc explicitly."),
    SparkPattern("PROC SORT NODUPKEY", "dropDuplicates(by_cols)", "Keep one row per BY key after deterministic ordering policy is defined."),
    SparkPattern("PROC FREQ", "groupBy(...).count()", "Frequency tables."),
    SparkPattern("PROC MEANS / SUMMARY", "groupBy(...).agg(...)", "Grouped numeric summaries."),
    SparkPattern("PROC CONTENTS", "printSchema / dtypes / describe", "Metadata inspection."),
    SparkPattern("DATA step SET append", "unionByName", "Use allowMissingColumns=True for different schemas."),
    SparkPattern("row number", "row_number over Window", "Use Window.orderBy or Window.partitionBy for BY-group row numbers."),
    SparkPattern("FIRST. / LAST.", "row_number / count over Window", "Window functions are needed for BY-group flags."),
    SparkPattern("PROC SQL", "spark.sql or DataFrame API joins", "Use Spark SQL for SQL-heavy code and DataFrame API for typed transformations."),
    SparkPattern("IF / IF-ELSE assignment", "when(...).otherwise(...)", "Conditional column creation."),
    SparkPattern("SELECT / WHEN", "chained when(...).when(...).otherwise(...)", "Categorization logic."),
]


SPARK_CANDIDATE_PROCS = {
    "sql",
    "sort",
    "freq",
    "means",
    "summary",
    "contents",
    "append",
    "rank",
    "transpose",
}


def spark_pattern_table() -> list[dict]:
    return [asdict(pattern) for pattern in SPARK_PATTERNS]


def build_spark_plan(manifest: dict) -> dict:
    proc_counts: dict[str, int] = {}
    for item in manifest.get("files", []):
        for proc in item.get("procs_used", []):
            proc_counts[proc] = proc_counts.get(proc, 0) + 1
    candidate_procs = [
        {"proc": proc, "count": count}
        for proc, count in sorted(proc_counts.items())
        if proc in SPARK_CANDIDATE_PROCS
    ]
    return {
        "spark_is_target_runtime": False,
        "recommendation": "Use PySpark for high-volume table transformations, SQL-heavy pipelines, and distributed execution targets.",
        "candidate_proc_count": sum(item["count"] for item in candidate_procs),
        "candidate_procs": candidate_procs,
        "patterns": spark_pattern_table(),
    }
