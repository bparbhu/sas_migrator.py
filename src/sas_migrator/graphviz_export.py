from __future__ import annotations

import html
from pathlib import Path
import shutil
import subprocess
import tempfile

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


def _fallback_svg(graph: nx.DiGraph, name: str, output_path: Path, message: str) -> None:
    nodes = list(graph.nodes(data=True))[:80]
    edges = list(graph.edges(data=True))[:120]
    line_height = 18
    height = max(220, 120 + line_height * (len(nodes) + len(edges) + 4))
    width = 1200
    lines = [
        '<svg xmlns="http://www.w3.org/2000/svg" width="{0}" height="{1}" viewBox="0 0 {0} {1}">'.format(width, height),
        '<rect width="100%" height="100%" fill="#ffffff"/>',
        '<text x="24" y="36" font-family="Arial" font-size="20" font-weight="700" fill="#111827">'
        + html.escape(name.replace("_", " ").title())
        + "</text>",
        '<text x="24" y="62" font-family="Arial" font-size="12" fill="#6b7280">'
        + html.escape(message)
        + "</text>",
        '<text x="24" y="92" font-family="Arial" font-size="14" font-weight="700" fill="#111827">Nodes</text>',
    ]
    y = 116
    for node, data in nodes:
        label = f'{data.get("kind", "node")}: {data.get("label", node)}'
        lines.append(
            f'<text x="36" y="{y}" font-family="Arial" font-size="12" fill="#374151">'
            + html.escape(_short_label(str(label), 150))
            + "</text>"
        )
        y += line_height
    y += 16
    lines.append(f'<text x="24" y="{y}" font-family="Arial" font-size="14" font-weight="700" fill="#111827">Edges</text>')
    y += 24
    for source, target, data in edges:
        label = f'{source} -> {target} ({data.get("kind", "edge")})'
        lines.append(
            f'<text x="36" y="{y}" font-family="Arial" font-size="12" fill="#374151">'
            + html.escape(_short_label(str(label), 150))
            + "</text>"
        )
        y += line_height
    lines.append("</svg>")
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


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


def write_graphviz_artifacts(graph: nx.DiGraph, output_base: Path, name: str, include_dot: bool = True) -> dict:
    """Write an SVG visualization, with optional DOT for audit/debug workflows."""
    output_base.mkdir(parents=True, exist_ok=True)
    dot = graph_to_dot(graph, name=name)
    dot_path = output_base / f"{name}.dot"
    if include_dot:
        dot_path.write_text(dot.source, encoding="utf-8")

    svg_path = output_base / f"{name}.svg"
    artifacts = {
        "dot": str(dot_path) if include_dot else None,
        "svg": str(svg_path),
        "rendered": False,
        "render_error": None,
    }

    if Digraph is not None:
        try:
            rendered_path = dot.render(filename=name, directory=str(output_base), format="svg", cleanup=not include_dot)
            source_path = output_base / name
            if include_dot and source_path.exists():
                source_path.unlink()
            artifacts["svg"] = rendered_path
            artifacts["rendered"] = True
            return artifacts
        except ExecutableNotFound as exc:
            artifacts["render_error"] = f"Graphviz executable not found: {exc}"
        except Exception as exc:
            artifacts["render_error"] = str(exc)

    dot_exe = shutil.which("dot")
    if dot_exe:
        try:
            with tempfile.NamedTemporaryFile("w", suffix=".dot", delete=False, encoding="utf-8") as handle:
                handle.write(dot.source)
                temp_dot = Path(handle.name)
            subprocess.run([dot_exe, "-Tsvg", str(temp_dot), "-o", str(svg_path)], check=True, capture_output=True, text=True)
            temp_dot.unlink(missing_ok=True)
            artifacts["rendered"] = True
            artifacts["render_error"] = None
            return artifacts
        except Exception as exc:
            artifacts["render_error"] = str(exc)

    _fallback_svg(
        graph,
        name,
        svg_path,
        artifacts["render_error"] or "Graphviz renderer unavailable; wrote readable SVG fallback.",
    )
    return artifacts


def write_translation_visualizations(
    output_root: Path,
    file_graph: nx.DiGraph,
    macro_graph: nx.DiGraph,
    migration_graph: nx.DiGraph,
    include_dot: bool = True,
) -> dict:
    viz_dir = output_root / "graphviz"
    return {
        "file_graph": write_graphviz_artifacts(file_graph, viz_dir, "file_graph", include_dot=include_dot),
        "macro_graph": write_graphviz_artifacts(macro_graph, viz_dir, "macro_graph", include_dot=include_dot),
        "migration_graph": write_graphviz_artifacts(migration_graph, viz_dir, "migration_graph", include_dot=include_dot),
    }
