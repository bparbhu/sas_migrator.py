# NetworkX Migration Graph

`sas-migrator` uses NetworkX to turn a SAS code tree into migration-planning graphs. The graph layer is deterministic and is built from the scanned manifest, not from an LLM.

## Graph Artifacts

Every `translate-tree` run now writes these graph artifacts to the output folder:

- `file_graph.json`: SAS file-to-file dependencies from `%include` and dataset producer/consumer relationships.
- `macro_graph.json`: macro definitions and macro callers.
- `migration_graph.json`: typed migration graph containing files, datasets, macros, PROCs, and librefs.
- `execution_plan.json`: topological file execution order when the file graph is acyclic.
- `parallel_batches.json`: groups of files that can be translated or reviewed in parallel.
- `graph_insights.json`: node/edge counts, high-degree nodes, datasets without local producers, and unused local outputs.
- `impact_report.json`: files affected by a change to a macro, dataset, file, or libref.

## Typed Graph Model

The typed graph uses stable node identifiers:

```text
file:jobs/sales/job1.sas
dataset:work.sales_local
macro:load_sales
proc:sql
libref:dw
```

Common edge types:

```text
includes
calls_macro
reads_dataset
writes_dataset
uses_proc
uses_libref
defines_macro
unresolved_include
unresolved_macro_call
```

Edge direction is chosen for impact analysis. For example, a dataset points to files that read it, and a file points to datasets it writes. This lets downstream traversal answer, "what breaks if this thing changes?"

## How To Generate

```bash
sas-migrator translate-tree examples/input_repo examples/generated_pandas --target pandas --strict
```

or planning only:

```bash
sas-migrator plan examples/input_repo --output-dir planning_output
```

## What This Enables

- Migration execution order
- Parallel translation batches
- Macro impact analysis
- Dataset lineage and missing upstream producer detection
- Identification of heavily connected/high-risk files and macros
- Triage of external inputs such as database tables or unresolved includes

## Notes

The graph is only as complete as the static scanner can infer. Dynamic macro-generated dataset names, runtime `%include` paths, and SAS code assembled from macro variables may require additional rules or manual review. Unsupported or ambiguous relationships should be surfaced in graph reports rather than silently guessed.
## Graphviz Visualization

Each `translate-tree` and `plan` run also writes Graphviz artifacts:

```text
graphviz_artifacts.json
graphviz/file_graph.dot
graphviz/macro_graph.dot
graphviz/migration_graph.dot
```

If the system Graphviz executable is installed and available on `PATH`, SVG files
are rendered beside the DOT files:

```text
graphviz/file_graph.svg
graphviz/macro_graph.svg
graphviz/migration_graph.svg
```

DOT files are always emitted because they only require the Python `graphviz`
package. SVG rendering is best-effort because the Python package delegates
rendering to the installed Graphviz system binary.