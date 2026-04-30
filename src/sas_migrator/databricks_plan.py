from __future__ import annotations


def build_databricks_plan(manifest: dict) -> dict:
    proc_counts: dict[str, int] = {}
    for item in manifest.get("files", []):
        for proc in item.get("procs_used", []):
            proc_counts[proc] = proc_counts.get(proc, 0) + 1
    return {
        "target": "databricks",
        "runtime": "Databricks Runtime with PySpark and Delta Lake",
        "recommended_artifacts": [
            "Databricks notebooks or .py tasks generated with --target databricks",
            "Databricks Jobs for orchestration",
            "Delta tables for durable outputs",
            "Unity Catalog catalog/schema mapping for SAS librefs",
            "MLflow tracking for migrated modeling workflows",
            "Delta Live Tables for declarative ETL where pipeline semantics are clear",
        ],
        "sas_to_databricks_concepts": [
            {"sas": "DATA step", "databricks": "PySpark DataFrame pipeline"},
            {"sas": "PROC SQL", "databricks": "spark.sql / Databricks SQL"},
            {"sas": "RUN / QUIT step boundary", "databricks": "lazy execution; optional checkpoint/cache/materialized Delta table"},
            {"sas": "SAS libraries / engines", "databricks": "Unity Catalog, Spark catalog, external locations, JDBC reads"},
            {"sas": "SAS/STAT modeling", "databricks": "Spark ML / MLflow / statsmodels when inference parity matters"},
            {"sas": "DI Studio flows", "databricks": "Databricks Jobs / Workflows / Delta Live Tables"},
            {"sas": "custom formats/informats", "databricks": "mapping tables, joins, UDFs, or custom format registry"},
        ],
        "warnings": [
            "Advanced row-iterative DATA step logic may not map cleanly to Spark's distributed execution model.",
            "Custom formats and informats need a dedicated registry or mapping-table strategy.",
            "Physical row order is not stable in Spark unless explicit ordering keys are used.",
            "Model diagnostics differ between SAS and Spark ML; validation criteria must be selected explicitly.",
        ],
        "proc_inventory": proc_counts,
    }
