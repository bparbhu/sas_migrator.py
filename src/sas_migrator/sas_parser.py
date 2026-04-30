from __future__ import annotations

import re

from .ir import (
    Assignment,
    ConditionalAssignment,
    DataStepNode,
    DatasetRef,
    ProcNode,
    ProcSqlNode,
    ProgramIR,
    UnsupportedNode,
)
from .translator import dataset_option_words, parse_merge_sources, split_blocks, statement_lines


def _dataset_ref(name: str, options: str = "", in_flag: str | None = None) -> DatasetRef:
    keep = dataset_option_words(options, "keep")
    drop = dataset_option_words(options, "drop")
    where = None
    rename = {}
    wm = re.search(r"where\s*=\s*\((.*?)\)", options, re.I)
    if wm:
        where = wm.group(1).strip()
    rm = re.search(r"rename\s*=\s*\((.*?)\)", options, re.I)
    if rm:
        rename = {
            a: b
            for a, b in re.findall(
                r"([A-Za-z_][A-Za-z0-9_]*)\s*=\s*([A-Za-z_][A-Za-z0-9_]*)",
                rm.group(1),
            )
        }
    return DatasetRef(name=name, keep=keep, drop=drop, where=where, rename=rename, in_flag=in_flag)


def parse_data_step(block: str) -> DataStepNode | UnsupportedNode:
    lines = statement_lines(block)
    if not lines:
        return UnsupportedNode(reason="empty data step", raw=block)
    target_match = re.match(r"data\s+([A-Za-z_][A-Za-z0-9_.]*)\s*;", lines[0], re.I)
    if not target_match:
        return UnsupportedNode(reason="DATA target could not be parsed", raw=block)
    source = None
    merge_sources = []
    by_cols = []
    where = None
    first_last_filter = None
    retain_counters = []
    assignments = []
    conditional_assignments = []
    merge_filter = None

    for line in lines[1:]:
        if line.lower() == "run;":
            continue
        set_match = re.match(r"set\s+([A-Za-z_][A-Za-z0-9_.]*)(\((.*?)\))?\s*;", line, re.I)
        if set_match:
            source = _dataset_ref(set_match.group(1), set_match.group(3) or "")
            continue
        merge_match = re.match(r"merge\s+(.+);", line, re.I)
        if merge_match:
            merge_sources = [
                _dataset_ref(dataset, options="", in_flag=in_flag)
                for dataset, in_flag in parse_merge_sources(merge_match.group(1))
            ]
            continue
        by_match = re.match(r"by\s+(.+);", line, re.I)
        if by_match:
            by_cols = by_match.group(1).split()
            continue
        fl_match = re.match(r"if\s+(first|last)\.([A-Za-z_][A-Za-z0-9_]*)\s*;", line, re.I)
        if fl_match:
            first_last_filter = (fl_match.group(1).upper(), fl_match.group(2))
            continue
        where_match = re.match(r"where\s+(.+);", line, re.I)
        if where_match:
            where = where_match.group(1).strip()
            continue
        retain_first = re.match(
            r"if\s+first\.([A-Za-z_][A-Za-z0-9_]*)\s+then\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*([0-9]+)\s*;",
            line,
            re.I,
        )
        if retain_first:
            retain_counters.append((retain_first.group(2), int(retain_first.group(3))))
            continue
        if re.match(r"else\s+[A-Za-z_][A-Za-z0-9_]*\s*\+\s*[0-9]+\s*;", line, re.I):
            continue
        if re.match(r"if\s+[A-Za-z_][A-Za-z0-9_]*(?:\s+and\s+[A-Za-z_][A-Za-z0-9_]*)*\s*;", line, re.I):
            merge_filter = line[:-1].strip()
            continue
        cond_assign = re.match(
            r"if\s+(.+?)\s+then\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.+?)\s*;\s*else\s+\2\s*=\s*(.+?)\s*;",
            line,
            re.I,
        )
        if cond_assign:
            conditional_assignments.append(
                ConditionalAssignment(
                    target=cond_assign.group(2),
                    condition=cond_assign.group(1).strip(),
                    when_true=cond_assign.group(3).strip(),
                    when_false=cond_assign.group(4).strip(),
                )
            )
            continue
        assign = re.match(r"([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.+);", line, re.I)
        if assign:
            assignments.append(Assignment(target=assign.group(1), expression=assign.group(2).strip()))

    return DataStepNode(
        target=target_match.group(1),
        source=source,
        merge_sources=merge_sources,
        by=by_cols,
        where=where,
        first_last_filter=first_last_filter,
        retain_counters=retain_counters,
        assignments=assignments,
        conditional_assignments=conditional_assignments,
        merge_filter=merge_filter,
    )


def parse_proc_sql(block: str) -> ProcSqlNode | UnsupportedNode:
    text = " ".join(line.strip() for line in block.splitlines())
    text = re.sub(r"\s+", " ", text).strip()
    match = re.search(
        r"create table ([A-Za-z_][A-Za-z0-9_.]*) as select (.+?) from (.+?)(?: where (.+?))?(?: group by (.+?))?(?: order by (.+?))?;",
        text,
        re.IGNORECASE,
    )
    if not match:
        return UnsupportedNode(reason="PROC SQL shape is not supported by parser", raw=block)
    target, select, from_clause, where, group_by, order_by = match.groups()
    return ProcSqlNode(target=target, select=select, from_clause=from_clause, where=where, group_by=group_by, order_by=order_by)


def parse_sas_to_ir(sas_code: str) -> ProgramIR:
    nodes = []
    for block in split_blocks(sas_code):
        low = block.strip().lower()
        if low.startswith("data "):
            nodes.append(parse_data_step(block))
        elif low.startswith("proc sql"):
            nodes.append(parse_proc_sql(block))
        elif low.startswith("proc "):
            name = low.split(None, 2)[1].strip(";")
            nodes.append(ProcNode(proc_name=name, raw=block))
        else:
            nodes.append(UnsupportedNode(reason="unknown block", raw=block))
    return ProgramIR(nodes=nodes)
