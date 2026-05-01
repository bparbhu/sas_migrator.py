from __future__ import annotations

from pathlib import Path

import networkx as nx

try:
    from graphviz import Digraph
    from graphviz.backend import ExecutableNotFound
except ImportError:  # Allows stale local envs to still write DOT via fallback.
    Digraph = None

    class ExecutableNotFound(Exception):
        pass


NODE_STYLES = {
    "file": {"shape": "box", "style": "rounded,filled", "fillcolor": "#dbeafe", "color": "#2563eb"},
    "dataset": {"shape": "cylinder", "style": "filled", "fillcolor": "#dcfce7", "color": "#16a34a"},
    "macro": {"shape": "component", "style": "filled", "fillcolor": "#fef3c7", "color": "#d97706"},
    "proc": {"shape": "hexagon", "style": "filled", "fillcolor": "#fce7f3", "color": "#db2777"},
    "libref": {"shape": "folder", "style": "filled", "fillcolor": "#ede9fe", "color": "#7c3aed"},
    "package": {"shape": "tab", "style": "filled", "fillcolor": "#e0f2fe", "color": "#0284c7"},
    "target": {"shape": "oval", "style": "filled", "fillcolor": "#fee2e2", "color": "#dc2626"},
    "function": {"shape": "note", "style": "filled", "fillcolor": "#f1f5f9", "color": "#475569"},
    "include": {"shape": "box", "style": "dashed,filled", "fillcolor": "#f8fafc", "color": "#64748b"},
}

EDGE_COLORS = {
    "include": "#2563eb",
    "includes": "#2563eb",
    "data": "#16a34a",
    "reads_dataset": "#16a34a",
    "writes_dataset": "#15803d",
    "macro_call": "#d97706",
    "calls_macro": "#d97706",
    "defines_macro": "#b45309",
    "uses_proc": "#db2777",
    "uses_libref": "#7c3aed",
    "maps_to_package": "#0284c7",
    "runs_on_target": "#dc2626",
    "unresolved_include": "#64748b",
    "unresolved_macro_call": "#64748b",
}


class _FallbackDot:
    def __init__(self, source: str):
        self.source = source


def _short_label(value: str, max_len: int = 46) -> str:
    label = value.replace("\\", "/")
    if len(label) <= max_len:
        return label
    return "..." + label[-(max_len - 3):]


def _quote_dot(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n") + '"'


def _fallback_dot(graph: nx.DiGraph, name: str) -> _FallbackDot:
    lines = [f"digraph {name} {{", "  rankdir=LR;"]
    for node, data in graph.nodes(data=True):
        label = data.get("label", node)
        kind = data.get("kind", "unknown")
        node_label = f"{kind}\\n{_short_label(str(label))}"
        lines.append(f"  {_quote_dot(str(node))} [label={_quote_dot(node_label)}];")
    for source, target, data in graph.edges(data=True):
        kind = data.get("kind", "edge")
        role = data.get("role")
        label = f"{kind}\\n{role}" if role else kind
        lines.append(f"  {_quote_dot(str(source))} -> {_quote_dot(str(target))} [label={_quote_dot(label)}];")
    lines.append("}")
    return _FallbackDot("\n".join(lines) + "\n")


def graph_to_dot(graph: nx.DiGraph, name: str = "sas_migration_graph"):
    if Digraph is None:
        return _fallback_dot(graph, name)

    dot = Digraph(name=name, graph_attr={"rankdir": "LR", "splines": "true", "overlap": "false"})
    dot.attr("node", fontname="Arial", fontsize="10")
    dot.attr("edge", fontname="Arial", fontsize="9")

    for node, data in graph.nodes(data=True):
        kind = data.get("kind", "unknown")
        label = data.get("label", node)
        attrs = dict(NODE_STYLES.get(kind, {"shape": "box", "style": "filled", "fillcolor": "#ffffff"}))
        tooltip = str(label)
        attrs["label"] = f"{kind}\n{_short_label(str(label))}" if kind != "unknown" else _short_label(str(label))
        attrs["tooltip"] = tooltip
        dot.node(str(node), **attrs)

    for source, target, data in graph.edges(data=True):
        kind = data.get("kind", "edge")
        label = kind
        role = data.get("role")
        if role:
            label = f"{kind}\n{role}"
        dot.edge(str(source), str(target), label=label, color=EDGE_COLORS.get(kind, "#334155"))
    return dot


def write_graphviz_artifacts(graph: nx.DiGraph, output_base: Path, name: str) -> dict:
    """Write DOT always and SVG when Graphviz Python/system support is available."""
    output_base.mkdir(parents=True, exist_ok=True)
    dot = graph_to_dot(graph, name=name)
    dot_path = output_base / f"{name}.dot"
    dot_path.write_text(dot.source, encoding="utf-8")

    artifacts = {"dot": str(dot_path), "svg": None, "rendered": False, "render_error": None}
    if Digraph is None:
        artifacts["render_error"] = "Python graphviz package is not installed; wrote DOT only."
        return artifacts

    try:
        rendered_path = dot.render(filename=name, directory=str(output_base), format="svg", cleanup=True)
        artifacts["svg"] = rendered_path
        artifacts["rendered"] = True
    except ExecutableNotFound as exc:
        artifacts["render_error"] = f"Graphviz executable not found: {exc}"
    except Exception as exc:
        artifacts["render_error"] = str(exc)
    return artifacts


def write_translation_visualizations(
    output_root: Path,
    file_graph: nx.DiGraph,
    macro_graph: nx.DiGraph,
    migration_graph: nx.DiGraph,
) -> dict:
    viz_dir = output_root / "graphviz"
    return {
        "file_graph": write_graphviz_artifacts(file_graph, viz_dir, "file_graph"),
        "macro_graph": write_graphviz_artifacts(macro_graph, viz_dir, "macro_graph"),
        "migration_graph": write_graphviz_artifacts(migration_graph, viz_dir, "migration_graph"),
    }