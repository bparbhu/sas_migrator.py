from __future__ import annotations
import json
from pathlib import Path
import networkx as nx

def build_file_graph(manifest: dict) -> nx.DiGraph:
    g = nx.DiGraph()
    files_by_name = {Path(f["file_path"]).name: f["file_path"] for f in manifest["files"]}
    producers = {}
    for item in manifest["files"]:
        g.add_node(item["file_path"], kind="file")
        for ds in item.get("datasets_written", []):
            producers[ds.lower()] = item["file_path"]
    for item in manifest["files"]:
        for include in item.get("includes", []):
            target = files_by_name.get(Path(include).name)
            if target:
                g.add_edge(target, item["file_path"], kind="include")
        for ds in item.get("datasets_read", []):
            prod = producers.get(ds.lower())
            if prod and prod != item["file_path"]:
                g.add_edge(prod, item["file_path"], kind="data")
    return g

def build_macro_graph(manifest: dict) -> nx.DiGraph:
    g = nx.DiGraph()
    macros = manifest.get("macros", {})
    for name, spec in macros.items():
        g.add_node(name, kind="macro", file=spec["file"])
    for item in manifest["files"]:
        f = item["file_path"]
        g.add_node(f, kind="file")
        for call in item.get("called_macros", []):
            if call in macros:
                g.add_edge(call, f, kind="macro_call")
    return g

def topological_execution_plan(file_graph: nx.DiGraph) -> dict:
    if nx.is_directed_acyclic_graph(file_graph):
        return {"is_dag": True, "order": list(nx.topological_sort(file_graph))}
    return {"is_dag": False, "cycles": [list(c) for c in nx.simple_cycles(file_graph)]}

def graph_to_json(graph: nx.DiGraph) -> dict:
    return {
        "nodes": [{"id": n, **graph.nodes[n]} for n in graph.nodes],
        "edges": [{"source": u, "target": v, **graph.edges[u, v]} for u, v in graph.edges],
    }

def save_json(data: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")
