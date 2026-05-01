# External SAS Corpus Testing

Use corpus testing when you want to evaluate `sas-migrator` against real SAS folders without treating every unsupported construct as a CI failure.

The public SAS examples repository at <https://github.com/sassoftware/sas-code-examples> is a useful first corpus. Its README describes it as a library of stand-alone SAS example programs organized by syntax and topic. Because it is external and can change over time, the GitHub workflow for it is manual rather than part of every pull request.

## Download The Public SAS Examples

```bash
python scripts/download_sas_code_examples.py external_corpora/sas-code-examples --overwrite
```

## Run A Corpus Test

```bash
sas-migrator corpus-test external_corpora/sas-code-examples \
  --output-dir corpus_output \
  --target pandas \
  --large-file-threshold 1000
```

The command writes:

```text
corpus_output/corpus_report.json
corpus_output/pandas/summary.json
corpus_output/pandas/manifest.json
corpus_output/pandas/migration_graph.json
corpus_output/pandas/graph_insights.json
corpus_output/pandas/impact_report.json
corpus_output/pandas/parallel_batches.json
```

## What The Corpus Report Measures

- number of SAS files
- total line count
- largest files
- files above the large-file threshold
- PROC frequency distribution
- package/target ecosystem routing
- unsupported issue counts
- warning issue counts
- pointers to graph artifacts

## Testing Thousands Of Lines

For large internal pipelines, point the same command at the root folder that contains related SAS programs and macros:

```bash
sas-migrator corpus-test D:\sas\claims_pipeline \
  --output-dir D:\sas_migration_reports\claims_pipeline \
  --target pandas \
  --large-file-threshold 1000
```

Run the command once per target when you want to compare pandas, PySpark, and Databricks generation quality:

```bash
sas-migrator corpus-test D:\sas\claims_pipeline --output-dir reports\claims_pandas --target pandas
sas-migrator corpus-test D:\sas\claims_pipeline --output-dir reports\claims_pyspark --target pyspark
sas-migrator corpus-test D:\sas\claims_pipeline --output-dir reports\claims_databricks --target databricks
```

## Manual GitHub Action

The `External SAS Corpus` workflow downloads the SAS examples repo and uploads the corpus report and graph artifacts. Run it from GitHub Actions when you want a fresh external-corpus signal.