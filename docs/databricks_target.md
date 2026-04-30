# Databricks Target

Databricks is not just generic PySpark. It adds a production platform around
Spark: notebooks, Jobs, Workflows, Delta Lake, Unity Catalog, Databricks SQL,
MLflow, and Delta Live Tables.

## Command

```bash
sas-migrator translate-tree <source_root> <output_root> --target databricks
```

This emits Databricks-friendly Python files using the PySpark emitter plus:

- Databricks notebook source header
- catalog/schema widgets
- `USE CATALOG` / `USE SCHEMA` setup
- checkpoint/materialization guidance
- Databricks-specific report issue

## SAS to Databricks Concepts

| SAS concept | Databricks concept |
| --- | --- |
| DATA step | PySpark DataFrame pipeline |
| PROC SQL | `spark.sql` / Databricks SQL |
| RUN / QUIT step boundary | Spark lazy execution; optional checkpoint/cache/materialized Delta table |
| SAS library / access engine | Unity Catalog, Spark catalog, external locations, JDBC reads |
| SAS/STAT modeling | Spark ML / MLflow; statsmodels when inference parity matters |
| DI Studio flow | Databricks Jobs / Workflows / Delta Live Tables |
| Custom formats/informats | mapping tables, joins, UDFs, or custom format registry |

## Production Notes

- Use Delta tables for durable outputs and repeatable validation.
- Use Unity Catalog to map SAS librefs to catalogs/schemas/external locations.
- Treat Spark as unordered unless explicit ordering is present.
- Use checkpointing, caching, or Delta materialization only at meaningful job
  boundaries; Spark is lazy by default.
- Advanced row-iterative DATA step logic may need a dedicated compatibility
  runtime or manual redesign.
- Use Spark ML for distributed predictive modeling and MLflow for tracking.
  Use statsmodels when the migration requires SAS-like inference tables.

## Source

- https://www.databricks.com/blog/2021/12/07/introduction-to-databricks-and-pyspark-for-sas-developers.html
