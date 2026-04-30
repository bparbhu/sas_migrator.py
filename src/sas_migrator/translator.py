from __future__ import annotations
import re
from .diagnostics import TranslationReport, TranslationResult
from .imports import build_pandas_imports
from .validator import validate_python

def split_blocks(text: str) -> list[str]:
    text = re.sub(r"(?is)\blibname\s+.*?;", "", text)
    text = re.sub(r"(?is)%include\s+['\"].*?['\"]\s*;", "", text)
    blocks, current = [], []
    in_proc_sql = False
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.lower().startswith("libname ") or line.lower().startswith("%include "):
            continue
        current.append(raw)
        low = line.lower()
        if low.startswith("proc sql"):
            in_proc_sql = True
            continue
        if in_proc_sql and low == "quit;":
            blocks.append("\n".join(current)); current = []; in_proc_sql = False; continue
        if not in_proc_sql and low == "run;":
            blocks.append("\n".join(current)); current = []
    if current:
        blocks.append("\n".join(current))
    return blocks

def statement_lines(block: str) -> list[str]:
    statements = []
    current = []
    quote = None
    for ch in block:
        current.append(ch)
        if ch in {"'", '"'}:
            quote = None if quote == ch else (ch if quote is None else quote)
        if ch == ";" and quote is None:
            statement = "".join(current).strip()
            if statement:
                statements.append(re.sub(r"\s+", " ", statement))
            current = []
    rest = "".join(current).strip()
    if rest:
        statements.extend(line.strip() for line in rest.splitlines() if line.strip())
    return statements

def expr_to_pd(expr: str, df: str) -> str:
    expr = expr.strip().rstrip(";")
    expr = re.sub(
        r"""'([0-9]{1,2}[A-Za-z]{3}[0-9]{2,4})'[dD]\b""",
        lambda m: f'__SAS_DATE_LITERAL_{m.group(1).upper()}__',
        expr,
    )
    strings = []
    def save(m):
        strings.append(m.group(0))
        return f"__STR{len(strings)-1}__"
    expr = re.sub(r"""'[^']*'|"[^"]*" """.strip(), save, expr)
    expr = re.sub(r"(?<![<>=!])=(?!=)", "==", expr)
    for a, b in [("and","&"),("or","|"),("not","~"),("ne","!="),("eq","=="),("gt",">"),("lt","<"),("ge",">="),("le","<=")]:
        expr = re.sub(rf"\b{a}\b", b, expr, flags=re.IGNORECASE)
    def repl(m):
        tok = m.group(0)
        if tok.startswith("__STR"):
            return tok
        if tok.startswith("__SAS_DATE_LITERAL_"):
            return tok
        if tok.lower() in {"year", "month", "day", "sas_year", "sas_month", "sas_day", "sas_date_literal", "sas_days_between"}:
            return tok
        if tok in {"True","False","None"}:
            return tok
        if m.start() > 0 and expr[m.start()-1] == ".":
            return tok
        return f'{df}["{tok}"]'
    expr = re.sub(r"\b[A-Za-z_][A-Za-z0-9_]*\b", repl, expr)
    for i, s in enumerate(strings):
        expr = expr.replace(f"__STR{i}__", s)
    expr = re.sub(r"__SAS_DATE_LITERAL_([A-Z0-9]+)__", r'sas_date_literal("\1")', expr)
    for sas_func, py_func in [("year", "sas_year"), ("month", "sas_month"), ("day", "sas_day")]:
        expr = re.sub(
            rf"\b{sas_func}\s*\(\s*{re.escape(df)}\[\"([A-Za-z_][A-Za-z0-9_]*)\"\]\s*\)",
            rf'{py_func}({df}["\1"])',
            expr,
            flags=re.IGNORECASE,
        )
        expr = re.sub(rf"\b{sas_func}\s*\(", f"{py_func}(", expr, flags=re.IGNORECASE)
    return expr

def split_dataset_name(name: str):
    if "." in name:
        a, b = name.split(".", 1)
        return a.lower(), b
    return None, name

def dataset_option_words(options: str, name: str) -> list[str]:
    m = re.search(
        rf"\b{name}\s*=\s*(.+?)(?=\s+\b(?:keep|drop|where|rename|obs|firstobs)\s*=|$)",
        options,
        re.IGNORECASE,
    )
    if not m:
        return []
    return [word for word in m.group(1).split() if re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", word)]

def split_sql_select(select_part: str) -> list[str]:
    parts = []
    current = []
    depth = 0
    for ch in select_part:
        if ch == "(":
            depth += 1
        elif ch == ")" and depth:
            depth -= 1
        if ch == "," and depth == 0:
            part = "".join(current).strip()
            if part:
                parts.append(part)
            current = []
        else:
            current.append(ch)
    part = "".join(current).strip()
    if part:
        parts.append(part)
    return parts

def strip_sql_aliases(expr: str) -> str:
    return re.sub(r"\b[A-Za-z_][A-Za-z0-9_]*\.", "", expr)

def sql_condition_to_pd(cond: str, df: str) -> str:
    cond = strip_sql_aliases(cond.strip())
    missing = re.match(r"([A-Za-z_][A-Za-z0-9_]*)\s*(=|is)\s*\.", cond, re.IGNORECASE)
    if missing:
        return f'{df}["{missing.group(1)}"].isna()'
    not_missing = re.match(r"([A-Za-z_][A-Za-z0-9_]*)\s+(?:ne|is\s+not)\s*\.", cond, re.IGNORECASE)
    if not_missing:
        return f'{df}["{not_missing.group(1)}"].notna()'
    return expr_to_pd(cond.replace("<>", "ne"), df)

def sql_value_to_pd(value: str, df: str) -> str:
    value = strip_sql_aliases(value.strip())
    if value == ".":
        return "pd.NA"
    if re.match(r"^'.*'$|^\".*\"$|^[+-]?\d+(?:\.\d+)?$", value):
        return value
    if re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", value):
        return f'{df}["{value}"]'
    return expr_to_pd(value, df)

def db_read_lines(dataset: str, var_name: str, where: str | None, db_librefs: dict):
    libref, table = split_dataset_name(dataset)
    if not libref or libref not in db_librefs:
        return None
    engine_name = db_librefs[libref]
    lines = []
    lines.append(
        f'{var_name} = read_database_table("{libref}", "{engine_name}", "{table}", {where!r})'
    )
    return lines

def parse_merge_sources(text: str):
    pattern = re.compile(r'([A-Za-z_][A-Za-z0-9_.]*)(?:\((.*?)\))?')
    sources = []
    for dataset, opts in pattern.findall(text):
        in_flag = None
        if opts:
            m = re.search(r'\bin\s*=\s*([A-Za-z_][A-Za-z0-9_]*)', opts, re.IGNORECASE)
            if m:
                in_flag = m.group(1)
        sources.append((dataset, in_flag))
    return sources

def infer_merge_how(filter_expr: str | None, flags: list[str]) -> str:
    if not filter_expr or len(flags) < 2:
        return "outer"
    lowered = filter_expr.lower().strip().rstrip(";")
    a, b = flags[0], flags[1]
    if f"{a} and {b}" in lowered:
        return "inner"
    if lowered == a:
        return "left"
    if lowered == b:
        return "right"
    return "outer"

def translate_proc_sql(block: str, db_librefs: dict, report: TranslationReport | None = None) -> list[str]:
    text = " ".join(line.strip() for line in block.splitlines())
    text = re.sub(r"\s+", " ", text).strip()
    m = re.search(r'create table ([A-Za-z_][A-Za-z0-9_.]*) as select (.+?) from (.+?)(?: where (.+?))?(?: group by (.+?))?(?: order by (.+?))?;', text, re.IGNORECASE)
    if not m:
        if report:
            report.add("warning", "unsupported_proc_sql", "PROC SQL shape is not supported yet.", "proc sql")
        return ["# unsupported PROC SQL block"]
    target, select_part, from_part, where_part, group_by_part, order_by_part = m.groups()
    target_var = target.replace(".", "_")
    lines = []

    join_match = re.search(r'([A-Za-z_][A-Za-z0-9_.]*)(?:\s+(?:as\s+)?([A-Za-z_][A-Za-z0-9_]*))?\s+(left|right|inner|full)?\s*join\s+([A-Za-z_][A-Za-z0-9_.]*)(?:\s+(?:as\s+)?([A-Za-z_][A-Za-z0-9_]*))?\s+on\s+(.+)', from_part, re.IGNORECASE)
    if join_match:
        left_ds, left_alias, how, right_ds, right_alias, on_clause = join_match.groups()
        left_alias = (left_alias or split_dataset_name(left_ds)[1]).lower()
        right_alias = (right_alias or split_dataset_name(right_ds)[1]).lower()
        left_var = left_ds.replace(".", "_")
        right_var = right_ds.replace(".", "_")
        for ds, var in [(left_ds, left_var), (right_ds, right_var)]:
            db_lines = db_read_lines(ds, var, None, db_librefs)
            if db_lines:
                lines.extend(db_lines)
        key_pairs = re.findall(r'([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\s*=\s*([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)', on_clause, re.IGNORECASE)
        left_on, right_on = [], []
        for a1, c1, a2, c2 in key_pairs:
            if a1.lower() == left_alias and a2.lower() == right_alias:
                left_on.append(c1); right_on.append(c2)
            elif a1.lower() == right_alias and a2.lower() == left_alias:
                left_on.append(c2); right_on.append(c1)
        merge_how = {"full":"outer"}.get((how or "inner").lower(), (how or "inner").lower())
        base_var = f"{target_var}_joined"
        lines.append(f"{base_var} = {left_var}.merge({right_var}, how={merge_how!r}, left_on={left_on!r}, right_on={right_on!r})")
    else:
        base_ds = from_part.split()[0]
        base_var = base_ds.replace(".", "_")
        left_var = base_var
        left_alias = split_dataset_name(base_ds)[1].lower()
        db_lines = db_read_lines(base_ds, base_var, None, db_librefs)
        if db_lines:
            lines.extend(db_lines)

    df_expr = base_var
    if where_part:
        lines.append(f"{target_var}_filtered = {df_expr}.loc[{sql_condition_to_pd(where_part, df_expr)}].copy()")
        df_expr = f"{target_var}_filtered"

    select_cols = split_sql_select(select_part)
    group_cols = [c.strip().split(".")[-1] for c in group_by_part.split(",")] if group_by_part else []
    agg_specs, simple_cols, case_specs, wildcard_aliases = [], [], [], []
    for col in select_cols:
        wildcard = re.match(r'([A-Za-z_][A-Za-z0-9_]*)\.\*$', col, re.IGNORECASE)
        if wildcard:
            wildcard_aliases.append(wildcard.group(1).lower())
            continue
        case = re.match(
            r'case\s+when\s+(.+?)\s+then\s+(.+?)\s+else\s+(.+?)\s+end\s+(?:as\s+)?([A-Za-z_][A-Za-z0-9_]*)$',
            col,
            re.IGNORECASE,
        )
        if case:
            cond, yes, no, alias = case.groups()
            case_specs.append((alias, cond, yes, no))
            continue
        agg = re.match(r'(sum|avg|mean|min|max|count)\((.*?)\)\s+(?:as\s+)?([A-Za-z_][A-Za-z0-9_]*)', col, re.IGNORECASE)
        if agg:
            func, inner, alias = agg.groups()
            func = {"avg":"mean","mean":"mean"}.get(func.lower(), func.lower())
            agg_specs.append((alias, inner.strip().split(".")[-1], func))
        else:
            plain = re.match(r'([A-Za-z_][A-Za-z0-9_.]*)(?:\s+(?:as\s+)?([A-Za-z_][A-Za-z0-9_]*))?$', col, re.IGNORECASE)
            if plain:
                src, alias = plain.groups()
                src = src.split(".")[-1]
                simple_cols.append((src, alias or src))

    if group_cols and agg_specs:
        lines.append(f"{target_var} = (")
        lines.append(f"    {df_expr}.groupby({group_cols!r}, as_index=False)")
        lines.append("    .agg(")
        for alias, src, func in agg_specs:
            lines.append(f'        {alias}=("{src}", "{func}"),')
        lines.append("    )")
        lines.append(")")
    else:
        if wildcard_aliases:
            if left_alias in wildcard_aliases:
                lines.append(f"{target_var} = {df_expr}.loc[:, list({left_var}.columns)].copy()")
            else:
                lines.append(f"{target_var} = {df_expr}.copy()")
        elif simple_cols:
            src_cols = [s for s, _ in simple_cols]
            lines.append(f"{target_var} = {df_expr}.loc[:, {src_cols!r}].copy()")
            rename_map = {s:a for s,a in simple_cols if s != a}
            if rename_map:
                lines.append(f"{target_var} = {target_var}.rename(columns={rename_map!r})")
        else:
            lines.append(f"{target_var} = {df_expr}.copy()")
        for alias, cond, yes, no in case_specs:
            lines.append(f'{target_var}["{alias}"] = {sql_value_to_pd(no, df_expr)}')
            lines.append(f'{target_var}.loc[{sql_condition_to_pd(cond, df_expr)}, "{alias}"] = {sql_value_to_pd(yes, df_expr)}')

    if order_by_part:
        order_items = [p.strip() for p in order_by_part.split(",")]
        by_cols, ascending = [], []
        for item in order_items:
            parts = item.split()
            by_cols.append(parts[0].split(".")[-1])
            ascending.append(not (len(parts) > 1 and parts[1].lower() == "desc"))
        lines.append(f"{target_var} = {target_var}.sort_values(by={by_cols!r}, ascending={ascending!r}).reset_index(drop=True)")
    return lines

def translate_with_report(sas_code: str, db_librefs: dict | None = None) -> TranslationResult:
    db_librefs = db_librefs or {}
    report = TranslationReport()
    out = []
    for block in split_blocks(sas_code):
        report.blocks_seen += 1
        low = block.strip().lower()
        if low.startswith("data "):
            lines = statement_lines(block)
            m = re.match(r"data\s+([A-Za-z_][A-Za-z0-9_.]*)\s*;", lines[0], re.I)
            if not m:
                report.add("warning", "unsupported_data_step", "DATA step target could not be parsed.", "data")
                out += ["# unsupported data block", ""]
                continue
            target = m.group(1).replace(".", "_")
            source = None
            keep, drop, where, rename = [], [], None, {}
            assigns, if_else = [], []
            merge_sources, by_cols, merge_filter = [], [], None
            first_last_filter = None
            retain_vars: dict[str, str] = {}
            retain_increment = None
            for line in lines[1:]:
                if line.lower() == "run;":
                    continue
                rm = re.match(r"retain\s+([A-Za-z_][A-Za-z0-9_]*)\s+(.+);", line, re.I)
                if rm:
                    retain_vars[rm.group(1)] = rm.group(2).strip()
                    continue
                sm = re.match(r"set\s+([A-Za-z_][A-Za-z0-9_.]*)(\((.*?)\))?\s*;", line, re.I)
                if sm:
                    source = sm.group(1)
                    opts = sm.group(3) or ""
                    keep = dataset_option_words(opts, "keep") or keep
                    drop = dataset_option_words(opts, "drop") or drop
                    wm = re.search(r"where\s*=\s*\((.*?)\)", opts, re.I)
                    if wm: where = wm.group(1).strip()
                    rm = re.search(r"rename\s*=\s*\((.*?)\)", opts, re.I)
                    if rm:
                        rename = {a:b for a,b in re.findall(r"([A-Za-z_][A-Za-z0-9_]*)\s*=\s*([A-Za-z_][A-Za-z0-9_]*)", rm.group(1))}
                    continue
                mm = re.match(r"merge\s+(.+);", line, re.I)
                if mm:
                    merge_sources = parse_merge_sources(mm.group(1))
                    continue
                bm = re.match(r"by\s+(.+);", line, re.I)
                if bm:
                    by_cols = bm.group(1).split()
                    continue
                fl = re.match(r"if\s+(first|last)\.([A-Za-z_][A-Za-z0-9_]*)\s*;", line, re.I)
                if fl:
                    first_last_filter = (fl.group(1).upper(), fl.group(2))
                    continue
                mf = re.match(r"if\s+[A-Za-z_][A-Za-z0-9_]*(?:\s+and\s+[A-Za-z_][A-Za-z0-9_]*)*\s*;", line, re.I)
                if mf:
                    merge_filter = line[:-1].strip()
                    continue
                rim = re.match(r"else\s+([A-Za-z_][A-Za-z0-9_]*)\s*\+\s*([0-9]+)\s*;", line, re.I)
                if rim:
                    retain_increment = (rim.group(1), int(rim.group(2)))
                    continue
                fim = re.match(r"if\s+first\.([A-Za-z_][A-Za-z0-9_]*)\s+then\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*([0-9]+)\s*;", line, re.I)
                if fim:
                    retain_increment = (fim.group(2), int(fim.group(3)))
                    continue
                wm = re.match(r"where\s+(.+);", line, re.I)
                if wm:
                    where = wm.group(1).strip()
                    continue
                im = re.match(r"if\s+(.+?)\s+then\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.+?)\s*;\s*else\s+\2\s*=\s*(.+?)\s*;", line, re.I)
                if im:
                    if_else.append((im.group(2), im.group(1).strip(), im.group(3).strip(), im.group(4).strip()))
                    continue
                lag = re.match(r"([A-Za-z_][A-Za-z0-9_]*)\s*=\s*lag\(([^)]+)\)\s*;", line, re.I)
                if lag:
                    assigns.append((lag.group(1), f'{target}["{lag.group(2)}"].shift(1)'))
                    continue
                am = re.match(r"([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.+);", line, re.I)
                if am:
                    assigns.append((am.group(1), am.group(2).strip()))
                    continue

            if merge_sources and by_cols:
                report.blocks_translated += 1
                flags = [flag for _, flag in merge_sources if flag]
                keep_flags = []
                if merge_filter:
                    keep_flags = [flag for flag in flags if re.search(rf"\b{re.escape(flag)}\b", merge_filter, re.I)]
                merge_inputs = [(flag or ds.replace(".", "_"), ds.replace(".", "_")) for ds, flag in merge_sources]
                input_expr = "[" + ", ".join(f"({flag!r}, {var})" for flag, var in merge_inputs) + "]"
                out.append(f"{target} = sas_style_merge({input_expr}, by={by_cols!r}, keep_flags={keep_flags!r})")
            elif source:
                report.blocks_translated += 1
                src_var = source.replace(".", "_")
                db_lines = db_read_lines(source, src_var, where, db_librefs)
                post_where = None if db_lines else where
                if db_lines:
                    out += db_lines
                out.append(f"{target} = {src_var}.copy()" if not keep else f"{target} = {src_var}.loc[:, {keep!r}].copy()")
                if drop: out.append(f"{target} = {target}.drop(columns={drop!r})")
                if rename: out.append(f"{target} = {target}.rename(columns={rename!r})")
                if post_where: out.append(f"{target} = {target}.loc[{expr_to_pd(post_where, target)}].copy()")
                if by_cols:
                    out.append(f"{target} = sas_first_last_flags({target}, by={by_cols!r})")
                if first_last_filter:
                    prefix, col = first_last_filter
                    out.append(f'{target} = {target}.loc[{target}["{prefix}_{col}"]].copy()')
                if retain_increment and by_cols:
                    retain_col, start = retain_increment
                    out.append(f'{target} = sas_retain_cumcount({target}, by={by_cols[:1]!r}, target="{retain_col}", start={start})')
            else:
                report.add("warning", "unsupported_data_step", "DATA step has no supported SET or MERGE source.", "data")
                out += ["# unsupported data block: no SET or MERGE", ""]
                continue

            for col, expr in assigns:
                if expr.startswith(f'{target}["'):
                    out.append(f'{target}["{col}"] = {expr}')
                else:
                    out.append(f'{target}["{col}"] = {expr_to_pd(expr, target)}')
            for col, cond, yes, no in if_else:
                out.append(f'{target}["{col}"] = {expr_to_pd(no, target)}')
                out.append(f'{target}.loc[{expr_to_pd(cond, target)}, "{col}"] = {expr_to_pd(yes, target)}')
            out.append("")
        elif low.startswith("proc sort"):
            lines = statement_lines(block)
            m = re.match(r"proc\s+sort\s+data\s*=\s*([A-Za-z_][A-Za-z0-9_.]*)(?:\s+out\s*=\s*([A-Za-z_][A-Za-z0-9_.]*))?\s*;", lines[0], re.I)
            if not m:
                report.add("warning", "unsupported_proc_sort", "PROC SORT source could not be parsed.", "proc sort")
                out += ["# unsupported proc sort", ""]
                continue
            src = m.group(1).replace(".", "_"); dest = (m.group(2) or m.group(1)).replace(".", "_")
            by_cols, ascending = [], []
            for line in lines[1:]:
                if line.lower().startswith("by "):
                    toks = line[:-1].split()[1:]; i = 0
                    while i < len(toks):
                        if toks[i].lower() == "descending":
                            by_cols.append(toks[i+1]); ascending.append(False); i += 2
                        else:
                            by_cols.append(toks[i]); ascending.append(True); i += 1
            out.append(f'{dest} = {src}.sort_values(by={by_cols!r}, ascending={ascending!r}).reset_index(drop=True)')
            out.append("")
            report.blocks_translated += 1
        elif low.startswith("proc freq"):
            lines = statement_lines(block)
            m = re.match(r"proc\s+freq\s+data\s*=\s*([A-Za-z_][A-Za-z0-9_.]*)", lines[0], re.I)
            if not m:
                report.add("warning", "unsupported_proc_freq", "PROC FREQ source could not be parsed.", "proc freq")
                out += ["# unsupported proc freq", ""]
                continue
            src = m.group(1).replace(".", "_"); out_name, tables = f"{src}_freq", []
            for line in lines[1:]:
                if line.lower().startswith("tables "):
                    table_part = line[:-1].split(None, 1)[1].split("/")[0].strip()
                    tables = [p.strip() for p in table_part.split("*")]
                    om = re.search(r"out\s*=\s*([A-Za-z_][A-Za-z0-9_.]*)", line, re.I)
                    if om: out_name = om.group(1).replace(".", "_")
            out.append(f"{out_name} = ({src}.groupby({tables!r}, as_index=False).size().rename(columns={{'size': 'count'}}))")
            out.append("")
            report.blocks_translated += 1
        elif low.startswith("proc means") or low.startswith("proc summary"):
            lines = statement_lines(block)
            m = re.match(r"proc\s+(means|summary)\s+data\s*=\s*([A-Za-z_][A-Za-z0-9_.]*)", lines[0], re.I)
            if not m:
                report.add("warning", "unsupported_proc_means", "PROC MEANS source could not be parsed.", "proc means")
                out += ["# unsupported proc means", ""]
                continue
            src = m.group(2).replace(".", "_"); class_cols, var_cols, out_name, metrics = [], [], f"{src}_means", ["mean"]
            for line in lines[1:]:
                low2 = line.lower()
                if low2.startswith("class "): class_cols = line[:-1].split()[1:]
                elif low2.startswith("var "): var_cols = line[:-1].split()[1:]
                elif low2.startswith("output "):
                    om = re.search(r"out\s*=\s*([A-Za-z_][A-Za-z0-9_.]*)", line, re.I)
                    if om: out_name = om.group(1).replace(".", "_")
                    metrics = []
                    for sas_metric, pd_metric in [("mean","mean"),("sum","sum"),("min","min"),("max","max"),("n","count")]:
                        if re.search(rf"\b{sas_metric}\b", line, re.I): metrics.append(pd_metric)
                    if not metrics: metrics = ["mean"]
            out.append(f"{out_name} = (")
            out.append(f"    {src}.groupby({class_cols!r}, as_index=False)")
            out.append("    .agg(")
            for col in var_cols:
                for metric in metrics:
                    out.append(f'        {col}_{metric}=("{col}", "{metric}"),')
            out.append("    )")
            out.append(")")
            out.append("")
            report.blocks_translated += 1
        elif low.startswith("proc transpose"):
            lines = statement_lines(block)
            m = re.match(r"proc\s+transpose\s+data\s*=\s*([A-Za-z_][A-Za-z0-9_.]*)(?:\s+out\s*=\s*([A-Za-z_][A-Za-z0-9_.]*))?", lines[0], re.I)
            if not m:
                report.add("warning", "unsupported_proc_transpose", "PROC TRANSPOSE source could not be parsed.", "proc transpose")
                out += ["# unsupported proc transpose", ""]
                continue
            src = m.group(1).replace(".", "_"); dest = (m.group(2) or f"{src}_transpose").replace(".", "_")
            by, id_col, var_col = [], None, None
            for line in lines[1:]:
                low2 = line.lower()
                if low2.startswith("by "): by = line[:-1].split()[1:]
                elif low2.startswith("id "): id_col = line[:-1].split()[1]
                elif low2.startswith("var "): var_col = line[:-1].split()[1]
            index_expr = repr(by[0] if len(by) == 1 else by)
            out.append(f'{dest} = ({src}.pivot(index={index_expr}, columns={id_col!r}, values={var_col!r}).reset_index())')
            out.append("")
            report.blocks_translated += 1
        elif low.startswith("proc sql"):
            before = len(report.issues)
            out.extend(translate_proc_sql(block, db_librefs, report))
            out.append("")
            if len(report.issues) == before:
                report.blocks_translated += 1
        elif low.startswith("proc stdize"):
            lines = statement_lines(block)
            header = lines[0]
            m = re.match(
                r"proc\s+stdize\s+data\s*=\s*([A-Za-z_][A-Za-z0-9_.]*)\s+out\s*=\s*([A-Za-z_][A-Za-z0-9_.]*)(.*?)\s*;",
                header,
                re.I,
            )
            if not m:
                report.add("warning", "unsupported_proc_stdize", "PROC STDIZE source/output could not be parsed.", "proc stdize")
                out += ["# unsupported proc stdize", ""]
                continue
            src = m.group(1).replace(".", "_")
            dest = m.group(2).replace(".", "_")
            options = m.group(3).lower()
            var_cols = []
            method = "mean"
            for line in lines[1:]:
                low2 = line.lower()
                if low2.startswith("var "):
                    var_cols = line[:-1].split()[1:]
                elif "repvalue" in low2:
                    rm = re.search(r"repvalue\s*=\s*(mean|median|mode)", line, re.I)
                    if rm:
                        method = rm.group(1).lower()
            if "reponly" not in options:
                report.add(
                    "warning",
                    "partial_proc_stdize",
                    "Only PROC STDIZE REPONLY missing-value replacement is translated; standardization options need review.",
                    "proc stdize",
                )
            out.append(f"{dest} = {src}.copy()")
            for col in var_cols:
                if method == "mean":
                    out.append(f'{dest}["{col}"] = {dest}["{col}"].fillna({dest}["{col}"].mean())')
                elif method == "median":
                    out.append(f'{dest}["{col}"] = {dest}["{col}"].fillna({dest}["{col}"].median())')
                else:
                    out.append(f'{dest}["{col}"] = {dest}["{col}"].fillna({dest}["{col}"].mode(dropna=True).iloc[0])')
            out.append("")
            report.blocks_translated += 1
        else:
            block_type = "unknown"
            if low.startswith("proc glm") or low.startswith("proc anova") or low.startswith("proc reg"):
                block_type = low.split(None, 2)[0] + " " + low.split(None, 2)[1]
                report.add(
                    "warning",
                    "unsupported_statistical_proc",
                    "Statistical PROC translation needs explicit validation of missing-value handling, degrees of freedom, and sum-of-squares defaults.",
                    block_type,
                )
            else:
                report.add("warning", "unsupported_block", "SAS block type is not supported yet.", block_type)
            out.append("# unsupported block")
            for line in block.splitlines():
                out.append("# " + line)
            out.append("")
    import_lines = build_pandas_imports(out)
    code = "\n".join(import_lines + [""] + out).strip() + "\n"
    validate_python(code, report)
    return TranslationResult(code=code, report=report)


def translate(sas_code: str, db_librefs: dict | None = None) -> str:
    return translate_with_report(sas_code, db_librefs).code
