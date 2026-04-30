# Contributing

Thanks for helping improve `sas-migrator`.

## Local setup

```bash
python -m venv .venv
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

For optional runtime coverage:

```bash
python -m pip install -e ".[spark,sasdata,modeling,ci]"
```

## Before opening a PR

Run the same core checks that CI runs:

```bash
python -m pytest -q
sas-migrator translate-tree examples/input_repo /tmp/sas_migrator_pandas --target pandas --strict
sas-migrator translate-tree examples/input_repo /tmp/sas_migrator_pyspark --target pyspark --strict
sas-migrator translate-tree examples/input_repo /tmp/sas_migrator_databricks --target databricks --strict
sas-migrator validate-fixture tests/fixtures/large_sas_pipeline \
  --reference tests/fixtures/large_sas_pipeline/reference_impl.py \
  --output-dir /tmp/sas_migrator_large_fixture \
  --report /tmp/sas_migrator_large_fixture_report.json \
  --target pandas \
  --strict
```

## Adding translation coverage

When adding support for a SAS construct:

1. Add or update parser/IR coverage if the construct needs structured representation.
2. Add pandas, PySpark, or Databricks emitter support as appropriate.
3. Add a focused unit test for the construct.
4. Add an equivalence fixture when semantics are subtle or high-risk.
5. Make unsupported or partial behavior explicit in reports.

Generated Python must remain deterministic. Avoid LLM-dependent behavior in the compiler path.