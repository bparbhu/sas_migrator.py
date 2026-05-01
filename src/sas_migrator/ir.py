from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Literal


@dataclass(frozen=True)
class DatasetRef:
    name: str
    keep: list[str] = field(default_factory=list)
    drop: list[str] = field(default_factory=list)
    where: str | None = None
    rename: dict[str, str] = field(default_factory=dict)
    in_flag: str | None = None


@dataclass(frozen=True)
class Assignment:
    target: str
    expression: str


@dataclass(frozen=True)
class ConditionalAssignment:
    target: str
    condition: str
    when_true: str
    when_false: str


@dataclass(frozen=True)
class SelectAssignment:
    selector: str
    target: str
    cases: list[tuple[list[str], str]] = field(default_factory=list)
    otherwise: str | None = None


@dataclass(frozen=True)
class DataStepNode:
    kind: Literal["data_step"] = "data_step"
    target: str = ""
    source: DatasetRef | None = None
    merge_sources: list[DatasetRef] = field(default_factory=list)
    by: list[str] = field(default_factory=list)
    where: str | None = None
    first_last_filter: tuple[str, str] | None = None
    retain_counters: list[tuple[str, int]] = field(default_factory=list)
    assignments: list[Assignment] = field(default_factory=list)
    conditional_assignments: list[ConditionalAssignment] = field(default_factory=list)
    select_assignments: list[SelectAssignment] = field(default_factory=list)
    output_rename: dict[str, str] = field(default_factory=dict)
    merge_filter: str | None = None


@dataclass(frozen=True)
class ProcSqlNode:
    kind: Literal["proc_sql"] = "proc_sql"
    target: str = ""
    select: str = ""
    from_clause: str = ""
    where: str | None = None
    group_by: str | None = None
    order_by: str | None = None


@dataclass(frozen=True)
class ProcNode:
    kind: Literal["proc"] = "proc"
    proc_name: str = ""
    raw: str = ""


@dataclass(frozen=True)
class UnsupportedNode:
    kind: Literal["unsupported"] = "unsupported"
    reason: str = ""
    raw: str = ""


IRNode = DataStepNode | ProcSqlNode | ProcNode | UnsupportedNode


@dataclass(frozen=True)
class ProgramIR:
    nodes: list[IRNode]

    def to_dict(self) -> dict:
        return {"nodes": [asdict(node) for node in self.nodes]}
