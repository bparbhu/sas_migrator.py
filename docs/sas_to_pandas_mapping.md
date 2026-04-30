# SAS to pandas Mapping Notes

This file is the implementation guide for the parser -> IR -> pandas emitter.
It captures SAS behaviors that must be treated as semantics, not syntax sugar.

## DATA Step BY Groups

SAS creates temporary `FIRST.variable` and `LAST.variable` values for each BY
variable. They are available during DATA step processing and are not written to
the output data set by default.

Python mapping:
- stable sort with original row order as a tie breaker
- group with `dropna=False`
- emit temporary `FIRST_<column>` and `LAST_<column>` columns when needed
- drop temporary columns when an output-cleaning pass is added

## DATA Step MERGE

SAS DATA step match-merge requires BY variables and is not a SQL Cartesian
many-to-many join. SAS documentation notes that the output count for each BY
group is based on the largest number of observations in that BY group across
input data sets.

Python mapping:
- use `sas_style_merge` runtime helper for DATA step `MERGE`
- preserve `IN=` temporary contribution flags for post-merge IF filtering
- avoid using naive chained `DataFrame.merge` for DATA step MERGE semantics

## IN= Dataset Option

`IN=` creates a temporary Boolean indicator for whether an input data set
contributed to the current observation. The flag is available to DATA step
statements and is not output by default.

Python mapping:
- represent `IN=` on `DatasetRef.in_flag` in IR
- runtime merge helper creates temporary `_in_<flag>` columns
- apply IF filters against those flags, then remove the temp columns

## RETAIN

SAS normally resets assignment-created variables to missing at each DATA step
iteration. `RETAIN` prevents that reset and can assign an initial value.

Python mapping:
- common BY-group counters can use `groupby(...).cumcount() + start`
- complex retained state should lower to a row iterator helper and be flagged in
  diagnostics until fully supported

## PROC SQL CASE And Missing Values

SAS PROC SQL `CASE` returns a row-level value; omitted ELSE produces missing.
SAS supports missing checks with `IS NULL`, `IS MISSING`, and numeric `.`.
SAS missing-value ordering and comparison semantics differ from ANSI SQL.

Python mapping:
- simple searched CASE maps to initialize-else then masked overwrite
- `col = .`, `col IS NULL`, and `col IS MISSING` map to `.isna()`
- `col ne .` and `col IS NOT NULL/MISSING` map to `.notna()`

## pandas Notes

pandas `merge` is SQL-style and explicitly differs from SQL when both key
columns contain nulls: null keys can match each other. The converter should use
runtime helpers where SAS requires different behavior.

`groupby(...).cumcount()` is the right primitive for SAS-like sequence counters
when the retained state is a simple BY-group counter.

## Sources

- SAS BY statement: https://support.sas.com/documentation/cdl/en/lrdict/64316/HTML/default/a000202968.htm
- SAS MERGE statement: https://support.sas.com/documentation/cdl/en/lrdict/64316/HTML/default/a000202970.htm
- SAS IN= data set option: https://support.sas.com/documentation/cdl/en/lrdict/64316/HTML/default/a000131134.htm
- SAS RETAIN statement: https://support.sas.com/documentation/cdl/en/lrdict/64316/HTML/default/a000214163.htm
- SAS PROC SQL CASE expression: https://support.sas.com/documentation/cdl/en/proc/61895/HTML/default/a002473682.htm
- SAS PROC SQL IS condition: https://support.sas.com/documentation/cdl/en/proc/61895/HTML/default/a002473690.htm
- pandas merge: https://pandas.pydata.org/docs/reference/api/pandas.merge.html
- pandas groupby: https://pandas.pydata.org/docs/reference/groupby.html
