# SASPy Lessons

SASPy is valuable, but not because it contains a SAS-to-Python translator.

SASPy is primarily:

- a connection/session layer for SAS 9.4 and SAS Viya
- a Python-to-SAS code generator for common methods
- a data exchange bridge between SAS data sets and pandas DataFrames
- a wrapper layer around SAS analytics capabilities

It requires a live SAS runtime. That makes it a poor fit as the core engine for
this migrator, because this project needs static conversion and should work
without concurrent SAS sessions.

## What We Can Cannibalize

### Dataset Option Vocabulary

SASPy exposes and documents a practical `dsopts` model:

- `where`
- `keep`
- `drop`
- `obs`
- `firstobs`
- `format`
- `encoding`

These map directly into our `DatasetRef` IR and target emitters.

### Type Conversion Rules

SASPy documentation makes the core SAS data-model constraint explicit:

- SAS has numeric double and fixed-width character values.
- SAS date/time/datetime are numeric values interpreted through formats.
- SAS missing values become `NaN` / `NaT` in pandas.
- pandas datetime values need explicit SAS formats if round-tripping back.

This informs our ingestion profiles, dtype optimization, and validation reports.

### Metadata Extraction

SASPy methods like `contents()` and `columnInfo()` submit `PROC CONTENTS` and
return metadata frames. We can mirror that offline by generating profile JSON
from converted SAS data files:

- row count
- column count
- dtype
- missing counts
- labels
- formats

### Parquet Strategy

SASPy includes SAS-to-Parquet support for large data sets that should not live
fully in pandas. This confirms our `ingest-sas-data` approach: convert raw SAS
files once to Parquet and reuse those outputs for pandas, Spark, and Databricks.

### Analytics Wrapper Inventory

SASPy's `sasstat`, `sasml`, and related wrappers do not translate SAS code to
Python. They do show which analytical domains SAS users expect:

- statistics
- machine learning
- econometrics
- quality control
- graphics/tabulation

That supports our PROC ecosystem routing to pandas, statsmodels, scikit-learn,
SciPy, PySpark, Databricks, lifelines, and manual-review buckets.

## Optional Future Use

SASPy can be useful as an optional validation adapter when a licensed SAS runtime
is available:

1. Run original SAS code through SASPy.
2. Export selected SAS outputs to pandas/Parquet.
3. Run generated Python.
4. Compare outputs with the equivalence harness.

That should be optional. The deterministic parser/IR/emitter remains the core.

## Sources

- https://github.com/sassoftware/saspy
- https://sassoftware.github.io/saspy/
- https://sassoftware.github.io/saspy/getting-started.html
- https://sassoftware.github.io/saspy/api.html
- https://sassoftware.github.io/saspy/advanced-topics.html
