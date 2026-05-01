# sas_migrator

A dependency-aware, no-LLM SAS-to-pandas migration repo.

Environment setup with Conda:
```bash
conda env create -f environment.yml
conda activate sas-migrator
```

Environment setup with pip:
```bash
python -m venv .venv
.venv\Scripts\activate
python -m pip install -r requirements.txt
```

Supported Python versions:
- Python 3.11+
- CI runs on Python 3.11, 3.12, and 3.13

Local verification:
```bash
python -m pytest -q
sas-migrator validate-fixture tests\fixtures\large_sas_pipeline ^
  --reference tests\fixtures\large_sas_pipeline\reference_impl.py ^
  --output-dir validation_output ^
  --report validation_report.json ^
  --target pandas ^
  --strict
python -m build
python -m twine check dist/*
```

Docker build:
```bash
docker build -t sas-migrator:latest .
docker run --rm sas-migrator:latest --help
```

Translate a mounted SAS repository with Docker:
```bash
docker run --rm ^
  -v C:\path\to\sas_repo:/workspace/input:ro ^
  -v C:\path\to\translated:/workspace/output ^
  sas-migrator:latest translate-tree /workspace/input /workspace/output --target pandas --strict
```

Use `--target pyspark` or `--target databricks` for Spark-oriented output. The
image installs the package with `spark`, `sasdata`, and `modeling` extras so the
CLI can support PySpark generation, SAS dataset ingestion, and migration
planning that routes SAS PROCs to scipy, numpy, scikit-learn, Spark ML, statsmodels, and
related packages.

Main command:
```bash
sas-migrator translate-tree <source_root> <output_root>
```

Quick folder-to-folder example:

```bash
python scripts/convert_sas_folder.py examples/input_repo examples/generated_pandas --target pandas --strict
```

That command recursively finds `.sas` files, preserves the same folder structure,
and writes generated Python plus audit artifacts into the output folder, including Graphviz DOT/SVG migration graph visualizations.

Examples:
- `examples/input_repo` contains a small SAS repository with macros, DB librefs,
  DATA steps, PROC SQL, PROC SORT, PROC FREQ, PROC MEANS, and MERGE.
- `examples/README.md` has pandas, PySpark, Databricks, Docker, and validation
  commands.
- `tests/fixtures/large_sas_pipeline` contains the larger equivalence fixture
  used by CI.

Choose the output runtime:
```bash
sas-migrator translate-tree <source_root> <output_root> --target pandas
sas-migrator translate-tree <source_root> <output_root> --target pyspark
sas-migrator translate-tree <source_root> <output_root> --target databricks
```

`pandas` is the default. `pyspark` emits Spark DataFrame API / Spark SQL code for
the supported IR and PROC patterns. `databricks` emits PySpark code with
Databricks notebook/catalog/checkpoint scaffolding.

It walks the source tree recursively, preserves the same relative folder structure in the output tree,
and writes translated `.py` files plus sidecar `.expanded.sas`, `.ir.json`, and `.report.json` files.

For CI or large batch migrations, use strict mode:
```bash
sas-migrator translate-tree <source_root> <output_root> --strict
```

Strict mode still writes artifacts, but exits non-zero when unsupported SAS or macro items are found.
Every file-level report includes:
- macro expansion issues
- parsed IR node counts
- translated block counts
- unsupported feature counts
- generated Python syntax validation

Equivalence validation for large fixtures:
```bash
sas-migrator validate-fixture tests\fixtures\large_sas_pipeline ^
  --reference tests\fixtures\large_sas_pipeline\reference_impl.py ^
  --output-dir validation_output ^
  --report validation_report.json ^
  --target pandas ^
  --strict
```

The reference module must expose:
- `input_tables() -> dict[str, pandas.DataFrame]`
- `expected_outputs() -> dict[str, pandas.DataFrame]`
- optional `sort_keys() -> dict[str, list[str]]`

For `pandas`, the validation command executes generated code and compares
DataFrames with tolerances. For `pyspark` and `databricks`, it currently
validates generation and Python syntax; Spark runtime equivalence can be layered
in where a local Spark or Databricks test cluster is available.

SAS data ingestion:
```bash
sas-migrator ingest-sas-data <sas_data_root> <parquet_output_root>
```

This converts `.sas7bdat` and `.xpt` files to Parquet, normalizes SAS-style
missing strings, optimizes dataframe dtypes, and writes profile JSON files with
row counts, column counts, missing-value counts, labels, and formats where
available.

SASPy lessons:
```bash
sas-migrator saspy-lessons --output saspy_mapping_summary.json
```

SASPy does not provide a static SAS-to-Python translator. The useful pieces are
its dataset-option vocabulary, SAS/pandas type-conversion caveats, metadata
patterns, Parquet/data exchange strategy, analytics wrapper inventory, and
optional SAS-backed baseline validation when a SAS runtime is available.

Planning artifacts include `ecosystem_plan.json`, which classifies SAS PROCs by
the Python ecosystem target they should map to: pandas, statsmodels,
scikit-learn, Spark ML, numpy, scipy, PySpark, lifelines, plotting/reporting packages, or manual review.
They also include `pyspark_plan.json`, which identifies Spark-suitable
transformations and the corresponding DataFrame API patterns.
They also include `databricks_plan.json`, which maps SAS platform concepts to
Databricks Jobs, Delta Lake, Unity Catalog, Spark ML, Databricks SQL, and DLT.
They also include `migration_readiness.json`, which records environment,
inventory, validation, workflow, performance, team-readiness, and optional
LLM-assisted-review workstreams.

Database-backed SAS librefs are translated to SQLAlchemy reads. Set one environment variable per libref:
```bash
SAS_MIGRATOR_DB_DW_URL=oracle+oracledb://user:password@host/service
```

Supported:
- `%let`
- `%macro ... %mend`
- simple `%if/%then/%else`
- simple `%do i=1 %to n`
- `DATA ... SET ...`
- BY-group `FIRST.` / `LAST.` filters using stable SAS-style ordering
- common `RETAIN` counter patterns for BY groups
- `MERGE ... BY`
- multi-way `MERGE` with `IN=` keep-flag filtering
- `PROC SORT`
- `PROC FREQ`
- `PROC MEANS`
- `PROC TRANSPOSE`
- first-pass `PROC SQL`
- `PROC SQL` left/right/inner/full joins with aliases, `alias.*`, simple `CASE WHEN ... THEN ... ELSE ... END AS`, and SAS missing checks such as `col = .`
- DB-backed `LIBNAME` detection with SQLAlchemy stubs
- SAS date helpers for `YEAR`, `MONTH`, `DAY`, and date literals like `'15JAN2023'D`
- statistical validation helpers for tolerances, listwise deletion, and SAS-style ANOVA type defaults

Architecture:
- SAS source is expanded through the macro layer.
- Expanded SAS is parsed into a JSON-serializable IR.
- Current pandas generation still uses the compatibility translator while the IR emitter is expanded.
- The IR files are emitted now so coverage and audits can be built construct by construct.

See `docs/sas_to_pandas_mapping.md` for the SAS-to-pandas semantic mapping notes and source links.
See `docs/sas_proc_ecosystem_mapping.md` for SAS PROC-to-Python-package routing.
See `docs/sas_function_numpy_scipy_mapping.md` for SAS function-to-NumPy/SciPy routing.
See `docs/sas_to_pyspark_mapping.md` for SAS-to-PySpark routing.
See `docs/databricks_target.md` for Databricks-specific output guidance.
See `docs/migration_program_readiness.md` for operating-model readiness guidance.
See `docs/migration_graph.md` for the NetworkX typed graph, impact, parallel-batch reports, and Graphviz DOT/SVG visualizations.
See `docs/external_corpus_testing.md` for testing against real SAS code corpora such as `sassoftware/sas-code-examples`.
See `docs/equivalence_validation.md` for generated-code equivalence testing.
See `docs/sas_data_ingestion.md` for raw SAS dataset conversion to Parquet.
See `docs/saspy_lessons.md` for what can and cannot be reused from SASPy.
See `docs/production_hardening.md` for the production hardening backlog.
See `CONTRIBUTING.md` and `SECURITY.md` for contribution and vulnerability-reporting guidance.

Important:
This is a working migration factory. It is not a complete SAS compiler. Unsupported constructs are surfaced in reports so large migrations can be triaged and improved rule by rule.
