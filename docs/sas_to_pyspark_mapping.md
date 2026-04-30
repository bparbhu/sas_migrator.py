# SAS to PySpark Mapping Notes

PySpark should be a first-class target when the migration needs distributed
execution. The pandas translator remains useful for local and medium-sized jobs,
but Spark is often the right backend for high-volume SAS pipelines.

## Common SAS to PySpark Patterns

| SAS construct | PySpark DataFrame API |
| --- | --- |
| DATA step assignment | `withColumn` |
| KEEP option | `select` |
| DROP option | `drop` |
| WHERE / subsetting IF | `filter` / `where` |
| PROC SORT BY | `orderBy` / `sort` |
| PROC SORT NODUPKEY | deterministic sort plus `dropDuplicates(by_cols)` |
| PROC FREQ | `groupBy(...).count()` |
| PROC MEANS / SUMMARY | `groupBy(...).agg(...)` |
| PROC CONTENTS | `printSchema`, `dtypes`, `describe` |
| DATA step SET append | `unionByName` |
| Append with different columns | `unionByName(..., allowMissingColumns=True)` |
| Row number | `row_number().over(Window.orderBy(...))` |
| Row number by groups | `row_number().over(Window.partitionBy(...).orderBy(...))` |
| FIRST. / LAST. | window `row_number` and group counts |
| PROC SQL | `spark.sql` or DataFrame joins |
| IF / IF-ELSE assignment | `when(...).otherwise(...)` |
| SELECT / WHEN categorization | chained `when(...).when(...).otherwise(...)` |
| PROC RANK groups | `ntile(n).over(Window.orderBy(...))` |

## Production Notes

- Spark DataFrames are unordered unless an order is explicitly applied. SAS code
  that depends on physical row order must be given a deterministic ordering key.
- `PROC SORT NODUPKEY` requires a deterministic keep-first policy. Sort first,
  then drop duplicates by the BY columns.
- SAS BY-group logic should map to Spark window functions, not to pandas-style
  groupby operations.
- `unionByName(..., allowMissingColumns=True)` is the closest Spark primitive for
  appending data sets with different schemas.
- For SQL-heavy SAS programs, preserving the SQL shape with `spark.sql` may be
  cleaner than forcing everything through chained DataFrame calls.

## Tool Support

The converter emits `pyspark_plan.json` during `translate-tree` and exposes:

```bash
sas-migrator spark-plan <source_root> --output pyspark_plan.json
```

This does not yet emit PySpark code. It identifies Spark-suitable patterns and
records the mapping checklist for future emitter work.

## Source

- https://github.com/apalominor/sas-to-pyspark-code-examples
