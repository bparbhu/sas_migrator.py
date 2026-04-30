# Equivalence Validation

The converter cannot assume generated code is correct just because it compiles.
Production migration needs deterministic equivalence checks.

## Fixture Contract

Create a reference module beside a SAS fixture folder. It should expose:

```python
def input_tables() -> dict[str, pandas.DataFrame]:
    ...

def expected_outputs() -> dict[str, pandas.DataFrame]:
    ...

def sort_keys() -> dict[str, list[str]]:
    ...
```

`input_tables` provides the DataFrames that generated pandas code expects.
`expected_outputs` encodes the SAS-equivalent results. `sort_keys` is optional
and avoids false negatives for unordered outputs.

## Command

```bash
sas-migrator validate-fixture <source_root> \
  --reference <reference_impl.py> \
  --output-dir <generated_output_dir> \
  --report <validation_report.json> \
  --target pandas \
  --strict
```

For `pandas`, the command:

1. translates the SAS folder
2. compiles generated Python
3. executes generated files in sorted order
4. compares named DataFrame outputs against the reference implementation
5. writes a JSON report

For `pyspark` and `databricks`, the command currently performs generation and
syntax validation. Runtime Spark equivalence should be enabled in CI where a
local Spark runtime or Databricks test cluster is available.

## Large Fixture

The repo includes `tests/fixtures/large_sas_pipeline`, covering:

- DATA step `SET`, `KEEP`, `WHERE`, and assignment
- `PROC SORT`
- BY-group `LAST.`
- `PROC SQL` aggregation
- `PROC FREQ`
- `PROC MEANS`
- DATA step `MERGE` with `IN=` flags

The test suite validates that generated pandas output matches the
SAS-equivalent reference outputs for seven DataFrames.
