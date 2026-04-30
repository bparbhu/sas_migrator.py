# SAS Data Ingestion

Code conversion is only half the migration. Raw SAS data files should be
converted once into efficient Python-native storage so pipelines do not spend
their lives repeatedly parsing `.sas7bdat` or transport files.

## Command

```bash
sas-migrator ingest-sas-data <sas_data_root> <parquet_output_root>
```

The command scans for `.sas7bdat` and `.xpt` files and writes:

- mirrored `.parquet` files
- sidecar `.profile.json` files
- `sas_data_ingest_summary.json`

## What It Preserves Or Reports

- row count
- column count
- pandas dtype after conversion
- missing counts and percentages
- SAS-style missing-like string values such as `.`, `.A`, `.Z`
- labels and formats when `pyreadstat` is available

## Why Parquet

The pyOAI notes emphasize that repeated SAS parsing is slower and less storage
efficient than converting once to dataframe-friendly storage. This migrator uses
Parquet with `pyarrow` because it is widely supported by pandas, Spark, and
Databricks.

## Useful Follow-ups

- Use profile JSON files to build migration QA dashboards.
- Treat SAS special missing values explicitly before modeling.
- Preserve variable labels and source-file provenance for analyst handoff.
- Convert low-cardinality strings to categorical dtypes for pandas workloads.
- Use the same Parquet outputs as inputs for pandas, PySpark, and Databricks
  validation pipelines.

## Sources

- https://github.com/cairo-lab/pyOAI/blob/main/notebooks/Convert%20SAS%20to%20Dataframes.ipynb
- https://github-wiki-see.page/m/cairo-lab/pyOAI/wiki
