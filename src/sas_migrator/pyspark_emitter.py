from __future__ import annotations

import re

from .diagnostics import TranslationReport, TranslationResult
from .ir import DataStepNode, ProcNode, ProcSqlNode, ProgramIR, UnsupportedNode
from .translator import statement_lines
from .validator import validate_python


def _var(name: str) -> str:
    return name.replace(".", "_")


def _is_external_table(name: str) -> bool:
    return "." in name and not name.lower().startswith("work.")


def _emit_source_if_needed(name: str, out: list[str], emitted_sources: set[str]) -> None:
    var = _var(name)
    if _is_external_table(name) and var not in emitted_sources:
        out.append(f'{var} = spark.table("{name}")  # Replace with configured Spark catalog/JDBC read if needed.')
        emitted_sources.add(var)


def _spark_expr(expr: str) -> str:
    expr = expr.strip().rstrip(";")
    expr = re.sub(r"\bne\b", "!=", expr, flags=re.IGNORECASE)
    expr = re.sub(r"\beq\b", "=", expr, flags=re.IGNORECASE)
    expr = re.sub(r"\bgt\b", ">", expr, flags=re.IGNORECASE)
    expr = re.sub(r"\blt\b", "<", expr, flags=re.IGNORECASE)
    expr = re.sub(r"\bge\b", ">=", expr, flags=re.IGNORECASE)
    expr = re.sub(r"\ble\b", "<=", expr, flags=re.IGNORECASE)
    expr = re.sub(r"\b[A-Za-z_][A-Za-z0-9_]*\.", "", expr)
    expr = re.sub(r"(?<![<>=!])=(?!=)", "=", expr)
    return expr


def _emit_data_step(node: DataStepNode, out: list[str], report: TranslationReport, emitted_sources: set[str]) -> None:
    target = _var(node.target)
    if node.merge_sources and node.by:
        inputs = []
        for source in node.merge_sources:
            flag = source.in_flag or _var(source.name)
            var = _var(source.name)
            _emit_source_if_needed(source.name, out, emitted_sources)
            marked = f"{var}__with_in_{flag}"
            out.append(f'{marked} = {var}.withColumn("_in_{flag}", F.lit(True))')
            inputs.append((flag, marked))
        current = inputs[0][1]
        for flag, right in inputs[1:]:
            current_next = f"{target}__join_{flag}"
            out.append(f"{current_next} = {current}.join({right}, on={node.by!r}, how='outer')")
            current = current_next
        keep_flags = []
        if node.merge_filter:
            keep_flags = [
                source.in_flag
                for source in node.merge_sources
                if source.in_flag and re.search(rf"\b{re.escape(source.in_flag)}\b", node.merge_filter, re.I)
            ]
        if keep_flags:
            cond = " & ".join([f'F.coalesce(F.col("_in_{flag}"), F.lit(False))' for flag in keep_flags])
            out.append(f"{target} = {current}.filter({cond})")
        else:
            out.append(f"{target} = {current}")
        drop_cols = [f"_in_{flag}" for flag, _ in inputs]
        out.append(f"{target} = {target}.drop(*{drop_cols!r})")
        report.blocks_translated += 1
        return

    if not node.source:
        report.add("warning", "unsupported_pyspark_data_step", "DATA step has no supported SET or MERGE source.", "data")
        out.append("# unsupported DATA step: no SET or MERGE source")
        return

    source = node.source
    src_var = _var(source.name)
    _emit_source_if_needed(source.name, out, emitted_sources)
    out.append(f"{target} = {src_var}")
    if source.keep:
        out.append(f"{target} = {target}.select(*{source.keep!r})")
    if source.drop:
        out.append(f"{target} = {target}.drop(*{source.drop!r})")
    where = source.where or node.where
    if where:
        out.append(f'{target} = {target}.filter(F.expr({_spark_expr(where)!r}))')
    if source.rename:
        for old, new in source.rename.items():
            out.append(f'{target} = {target}.withColumnRenamed("{old}", "{new}")')
    if node.by:
        order_cols = ", ".join([f'F.col("{col}")' for col in node.by])
        for idx, col in enumerate(node.by):
            partition = ", ".join([f'F.col("{p}")' for p in node.by[: idx + 1]])
            win = f"{target}__w_{col}"
            out.append(f"{win} = Window.partitionBy({partition}).orderBy({order_cols})")
            out.append(f'{target} = {target}.withColumn("FIRST_{col}", F.row_number().over({win}) == 1)')
            out.append(f'{target} = {target}.withColumn("LAST_{col}", F.row_number().over({win}) == F.count(F.lit(1)).over({win}.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)))')
    if node.first_last_filter:
        prefix, col = node.first_last_filter
        out.append(f'{target} = {target}.filter(F.col("{prefix}_{col}"))')
    for retain_col, start in node.retain_counters:
        if node.by:
            partition = ", ".join([f'F.col("{node.by[0]}")'])
            win = f"{target}__retain_w_{retain_col}"
            out.append(f"{win} = Window.partitionBy({partition}).orderBy({order_cols})")
            out.append(f'{target} = {target}.withColumn("{retain_col}", F.row_number().over({win}) + {start - 1})')
    for assignment in node.assignments:
        out.append(f'{target} = {target}.withColumn("{assignment.target}", F.expr({_spark_expr(assignment.expression)!r}))')
    for assignment in node.conditional_assignments:
        out.append(
            f'{target} = {target}.withColumn("{assignment.target}", '
            f'F.when(F.expr({_spark_expr(assignment.condition)!r}), F.expr({_spark_expr(assignment.when_true)!r}))'
            f'.otherwise(F.expr({_spark_expr(assignment.when_false)!r})))'
        )
    report.blocks_translated += 1


def _emit_proc_sql(node: ProcSqlNode, out: list[str], report: TranslationReport) -> None:
    target = _var(node.target)
    sql = f"select {node.select} from {node.from_clause}"
    if node.where:
        sql += f" where {node.where}"
    if node.group_by:
        sql += f" group by {node.group_by}"
    if node.order_by:
        sql += f" order by {node.order_by}"
    out.append("# Register source DataFrames as Spark temp views before running this query when needed.")
    out.append(f'{target} = spark.sql("""{sql}""")')
    report.blocks_translated += 1


def _emit_proc_node(node: ProcNode, out: list[str], report: TranslationReport) -> None:
    proc = node.proc_name.lower()
    lines = statement_lines(node.raw)
    if proc == "sort":
        m = re.match(
            r"proc\s+sort\s+data\s*=\s*([A-Za-z_][A-Za-z0-9_.]*)(?:\s+out\s*=\s*([A-Za-z_][A-Za-z0-9_.]*))?(.*?)\s*;",
            lines[0],
            re.I,
        )
        if not m:
            report.add("warning", "unsupported_pyspark_proc_sort", "PROC SORT source could not be parsed.", "proc sort")
            out.append("# unsupported PROC SORT")
            return
        src = _var(m.group(1))
        dest = _var(m.group(2) or m.group(1))
        options = m.group(3).lower()
        order_exprs = []
        by_cols = []
        for line in lines[1:]:
            if line.lower().startswith("by "):
                toks = line[:-1].split()[1:]
                idx = 0
                while idx < len(toks):
                    if toks[idx].lower() == "descending":
                        by_cols.append(toks[idx + 1])
                        order_exprs.append(f'F.col("{toks[idx + 1]}").desc()')
                        idx += 2
                    else:
                        by_cols.append(toks[idx])
                        order_exprs.append(f'F.col("{toks[idx]}").asc()')
                        idx += 1
        out.append(f"{dest} = {src}.orderBy({', '.join(order_exprs)})")
        if "nodupkey" in options:
            out.append(f"{dest} = {dest}.dropDuplicates({by_cols!r})")
        report.blocks_translated += 1
        return
    if proc == "freq":
        m = re.match(r"proc\s+freq\s+data\s*=\s*([A-Za-z_][A-Za-z0-9_.]*)", lines[0], re.I)
        if not m:
            report.add("warning", "unsupported_pyspark_proc_freq", "PROC FREQ source could not be parsed.", "proc freq")
            out.append("# unsupported PROC FREQ")
            return
        src = _var(m.group(1))
        out_name = f"{src}_freq"
        table_cols = []
        for line in lines[1:]:
            if line.lower().startswith("tables "):
                table_part = line[:-1].split(None, 1)[1].split("/")[0].strip()
                table_cols = [part.strip() for part in table_part.split("*")]
                om = re.search(r"out\s*=\s*([A-Za-z_][A-Za-z0-9_.]*)", line, re.I)
                if om:
                    out_name = _var(om.group(1))
        out.append(f"{out_name} = {src}.groupBy(*{table_cols!r}).count()")
        report.blocks_translated += 1
        return
    if proc in {"means", "summary"}:
        m = re.match(r"proc\s+(means|summary)\s+data\s*=\s*([A-Za-z_][A-Za-z0-9_.]*)", lines[0], re.I)
        if not m:
            report.add("warning", "unsupported_pyspark_proc_means", "PROC MEANS source could not be parsed.", "proc means")
            out.append("# unsupported PROC MEANS")
            return
        src = _var(m.group(2))
        class_cols, var_cols, out_name, metrics = [], [], f"{src}_means", ["mean"]
        for line in lines[1:]:
            low = line.lower()
            if low.startswith("class "):
                class_cols = line[:-1].split()[1:]
            elif low.startswith("var "):
                var_cols = line[:-1].split()[1:]
            elif low.startswith("output "):
                om = re.search(r"out\s*=\s*([A-Za-z_][A-Za-z0-9_.]*)", line, re.I)
                if om:
                    out_name = _var(om.group(1))
                metrics = [metric for metric in ["mean", "sum", "min", "max", "count"] if re.search(rf"\b{metric if metric != 'count' else 'n'}\b", line, re.I)]
                if not metrics:
                    metrics = ["mean"]
        agg_exprs = []
        for col in var_cols:
            for metric in metrics:
                agg_name = "count" if metric == "count" else metric
                agg_exprs.append(f'F.{agg_name}("{col}").alias("{col}_{metric}")')
        out.append(f"{out_name} = {src}.groupBy(*{class_cols!r}).agg({', '.join(agg_exprs)})")
        report.blocks_translated += 1
        return
    if proc == "stdize":
        m = re.match(
            r"proc\s+stdize\s+data\s*=\s*([A-Za-z_][A-Za-z0-9_.]*)\s+out\s*=\s*([A-Za-z_][A-Za-z0-9_.]*)(.*?)\s*;",
            lines[0],
            re.I,
        )
        if not m:
            report.add("warning", "unsupported_pyspark_proc_stdize", "PROC STDIZE source/output could not be parsed.", "proc stdize")
            out.append("# unsupported PROC STDIZE")
            return
        src = _var(m.group(1))
        dest = _var(m.group(2))
        options = m.group(3).lower()
        var_cols = []
        method = "mean"
        for line in lines[1:]:
            low = line.lower()
            if low.startswith("var "):
                var_cols = line[:-1].split()[1:]
            elif "repvalue" in low:
                rm = re.search(r"repvalue\s*=\s*(mean|median|mode)", line, re.I)
                if rm:
                    method = rm.group(1).lower()
        if "reponly" not in options:
            report.add("warning", "partial_pyspark_proc_stdize", "Only PROC STDIZE REPONLY missing-value replacement is translated.", "proc stdize")
        out.append(f"{dest} = {src}")
        for col in var_cols:
            fill_var = f"{dest}_{col}_{method}"
            if method == "median":
                out.append(f'{fill_var} = {src}.approxQuantile("{col}", [0.5], 0.001)[0]')
            elif method == "mode":
                out.append(f'{fill_var} = {src}.groupBy("{col}").count().orderBy(F.col("count").desc()).first()[0]')
            else:
                out.append(f'{fill_var} = {src}.select(F.mean("{col}")).first()[0]')
            out.append(f'{dest} = {dest}.fillna({{"{col}": {fill_var}}})')
        report.blocks_translated += 1
        return
    report.add("warning", "unsupported_pyspark_proc", f"PROC {proc.upper()} is not supported by the PySpark emitter yet.", f"proc {proc}")
    out.append(f"# unsupported PROC {proc.upper()} for PySpark")


def emit_pyspark(program: ProgramIR) -> TranslationResult:
    report = TranslationReport(blocks_seen=len(program.nodes))
    out = [
        "from pyspark.sql import SparkSession, Window",
        "from pyspark.sql import functions as F",
        "",
        "spark = SparkSession.builder.getOrCreate()",
        "",
    ]
    emitted_sources: set[str] = set()
    for node in program.nodes:
        if isinstance(node, DataStepNode):
            _emit_data_step(node, out, report, emitted_sources)
            out.append("")
        elif isinstance(node, ProcSqlNode):
            _emit_proc_sql(node, out, report)
            out.append("")
        elif isinstance(node, ProcNode):
            _emit_proc_node(node, out, report)
            out.append("")
        elif isinstance(node, UnsupportedNode):
            report.add("warning", "unsupported_pyspark_block", node.reason, "unknown")
            out.append("# unsupported block")
            out.append("")
    code = "\n".join(out).strip() + "\n"
    validate_python(code, report)
    return TranslationResult(code=code, report=report)
