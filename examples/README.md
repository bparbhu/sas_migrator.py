# Examples

This folder contains a small SAS repository and generated-output examples you can
use to smoke-test the migrator before pointing it at a larger codebase.

## Input Repository

`input_repo` is intentionally shaped like a real SAS code tree:

```text
input_repo/
  macros/common.sas
  jobs/sales/job1.sas
  jobs/ops/job2.sas
  jobs/sql/job3.sas
  jobs/merge/job4.sas
```

It exercises:

- `%include`
- `%let`
- `%macro ... %mend`
- simple `%if/%then/%else`
- database-backed `LIBNAME`
- `DATA ... SET ...`
- SAS dataset options: `keep=` and `where=`
- `PROC SORT`
- `PROC FREQ`
- `PROC MEANS`
- `PROC SQL`
- `MERGE ... BY` with `IN=` flags


## Easiest Folder-to-Folder Conversion

Run this from the repository root to convert the bundled SAS example folder into
mirrored Python output:

```bash
python scripts/convert_sas_folder.py
```

That default command reads:

```text
examples/input_repo
```

and writes:

```text
examples/generated_pandas
```

Use your own SAS folder and output folder:

```bash
python scripts/convert_sas_folder.py /path/to/sas_code /path/to/python_output --target pandas --strict
```

PowerShell example:

```powershell
.\examples\convert_folder_example.ps1 `
  -SourceRoot "C:\path\to\sas_code" `
  -OutputRoot "C:\path\to\python_output" `
  -Target pandas `
  -Strict
```

Bash example:

```bash
bash examples/convert_folder_example.sh /path/to/sas_code /path/to/python_output pandas
```

The output folder preserves the input folder structure and writes one `.py` file
per `.sas` file, plus `.expanded.sas`, `.ir.json`, `.report.json`, and repository
level planning reports, including NetworkX graph artifacts for lineage, impact analysis, and parallel execution batches.

## Generate Pandas Output

```bash
sas-migrator translate-tree examples/input_repo examples/generated_pandas --target pandas --strict
```

Expected artifacts include mirrored Python files plus audit sidecars:

```text
examples/generated_pandas/
  jobs/sales/job1.py
  jobs/sales/job1.expanded.sas
  jobs/sales/job1.ir.json
  jobs/sales/job1.report.json
  manifest.json
  file_graph.json
  macro_graph.json
  execution_plan.json
  ecosystem_plan.json
  migration_readiness.json
```

## Generate PySpark Output

```bash
sas-migrator translate-tree examples/input_repo examples/generated_pyspark --target pyspark --strict
```

Use this when the SAS workload is closer to distributed ETL or when generated
code will be reviewed before moving into Spark jobs.

## Generate Databricks Output

```bash
sas-migrator translate-tree examples/input_repo examples/generated_databricks --target databricks --strict
```

The Databricks target emits PySpark-oriented code with Databricks-specific
notebook, catalog, and checkpoint scaffolding where supported.

## Docker Example

Build the image:

```bash
docker build -t sas-migrator:latest .
```

Run the same pandas example using mounted folders:

```bash
docker run --rm \
  -v "$PWD/examples/input_repo:/workspace/input:ro" \
  -v "$PWD/examples/docker_output:/workspace/output" \
  sas-migrator:latest translate-tree /workspace/input /workspace/output --target pandas --strict
```

PowerShell equivalent:

```powershell
docker run --rm `
  -v "${PWD}\examples\input_repo:/workspace/input:ro" `
  -v "${PWD}\examples\docker_output:/workspace/output" `
  sas-migrator:latest translate-tree /workspace/input /workspace/output --target pandas --strict
```

## Large Equivalence Fixture

The production-grade validation example lives under
`tests/fixtures/large_sas_pipeline`. It includes multiple related SAS programs
and a Python reference implementation that represents the SAS-equivalent output.

Run it with:

```bash
sas-migrator validate-fixture tests/fixtures/large_sas_pipeline \
  --reference tests/fixtures/large_sas_pipeline/reference_impl.py \
  --output-dir validation_output \
  --report validation_report.json \
  --target pandas \
  --strict
```

That command converts the fixture, executes the generated pandas code, and
compares generated tables against the reference outputs.
