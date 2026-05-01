from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path

import networkx as nx

from .function_registry import FUNCTION_MAPPINGS
from .proc_registry import classify_proc


def build_file_graph(manifest: dict) -> nx.DiGraph:
    graph = nx.DiGraph()
    files_by_name = {Path(f["file_path"]).name: f["file_path"] for f in manifest["files"]}
    producers = {}
    for item in manifest["files"]:
        graph.add_node(item["file_path"], kind="file")
        for dataset in item.get("datasets_written", []):
            producers[dataset.lower()] = item["file_path"]
    for item in manifest["files"]:
        for include in item.get("includes", []):
            target = files_by_name.get(Path(include).name)
            if target:
                graph.add_edge(target, item["file_path"], kind="include")
        for dataset in item.get("datasets_read", []):
            producer = producers.get(dataset.lower())
            if producer and producer != item["file_path"]:
                graph.add_edge(producer, item["file_path"], kind="data")
    return graph


def build_macro_graph(manifest: dict) -> nx.DiGraph:
    graph = nx.DiGraph()
    macros = manifest.get("macros", {})
    for name, spec in macros.items():
        graph.add_node(name, kind="macro", file=spec["file"])
    for item in manifest["files"]:
        file_path = item["file_path"]
        graph.add_node(file_path, kind="file")
        for call in item.get("called_macros", []):
            if call in macros:
                graph.add_edge(call, file_path, kind="macro_call")
    return graph


def _node_id(kind: str, value: str) -> str:
    return f"{kind}:{value}"


def _dataset_id(name: str) -> str:
    return _node_id("dataset", name.lower())


def _file_id(path: str) -> str:
    return _node_id("file", path)


def _macro_id(name: str) -> str:
    return _node_id("macro", name.lower())


def _proc_id(name: str) -> str:
    return _node_id("proc", name.lower())


def _libref_id(name: str) -> str:
    return _node_id("libref", name.lower())


def _package_id(name: str) -> str:
    return _node_id("package", name.lower())


def _target_id(name: str) -> str:
    return _node_id("target", name.lower())


def _function_id(name: str) -> str:
    return _node_id("function", name.lower())


def _package_to_target(package_name: str) -> str:
    normalized = package_name.lower()
    if normalized in {"pandas", "sqlalchemy", "pyreadstat", "jinja2", "matplotlib", "seaborn"}:
        return "pandas"
    if normalized in {"pyspark", "spark ml"}:
        return "pyspark"
    if normalized in {"databricks", "delta lake", "unity catalog"}:
        return "databricks"
    if normalized in {
        "numpy",
        "scipy",
        "scipy.stats",
        "scipy.linalg",
        "scipy.optimize",
        "statsmodels",
        "scikit-learn",
        "sklearn.decomposition",
        "lifelines",
        "scikit-survival",
        "prophet",
        "factor_analyzer",
        "cvxpy",
        "manual_review",
    }:
        return "python_ecosystem"
    return "manual_review"


def build_migration_graph(manifest: dict) -> nx.DiGraph:
    """Build a typed lineage and migration-capability graph."""
    graph = nx.DiGraph(root=manifest.get("root"))
    files_by_name = {Path(f["file_path"]).name: f["file_path"] for f in manifest.get("files", [])}

    for item in manifest.get("files", []):
        file_path = item["file_path"]
        file_node = _file_id(file_path)
        graph.add_node(file_node, kind="file", label=file_path, path=file_path)

        for include in item.get("includes", []):
            included_path = files_by_name.get(Path(include).name)
            if included_path:
                include_node = _file_id(included_path)
                graph.add_node(include_node, kind="file", label=included_path, path=included_path)
                graph.add_edge(include_node, file_node, kind="includes", include=include)
            else:
                include_node = _node_id("include", include)
                graph.add_node(include_node, kind="include", label=include, path=include, resolved=False)
                graph.add_edge(include_node, file_node, kind="unresolved_include", include=include)

        for dataset in item.get("datasets_read", []):
            dataset_node = _dataset_id(dataset)
            graph.add_node(dataset_node, kind="dataset", label=dataset, name=dataset.lower())
            graph.add_edge(dataset_node, file_node, kind="reads_dataset")

        for dataset in item.get("datasets_written", []):
            dataset_node = _dataset_id(dataset)
            graph.add_node(dataset_node, kind="dataset", label=dataset, name=dataset.lower())
            graph.add_edge(file_node, dataset_node, kind="writes_dataset")

        for proc in item.get("procs_used", []):
            proc_node = _proc_id(proc)
            mapping = classify_proc(proc)
            graph.add_node(
                proc_node,
                kind="proc",
                label=proc.upper(),
                name=proc.lower(),
                category=mapping.category,
                confidence=mapping.confidence,
                notes=mapping.notes,
            )
            graph.add_edge(proc_node, file_node, kind="uses_proc")
            for package_name, role in [
                (mapping.primary_package, "primary"),
                (mapping.fallback_package, "fallback"),
                (mapping.distributed_package, "distributed"),
            ]:
                if package_name:
                    package_node = _package_id(package_name)
                    target_name = _package_to_target(package_name)
                    target_node = _target_id(target_name)
                    graph.add_node(package_node, kind="package", label=package_name, name=package_name.lower())
                    graph.add_node(target_node, kind="target", label=target_name, name=target_name.lower())
                    graph.add_edge(proc_node, package_node, kind="maps_to_package", role=role, confidence=mapping.confidence)
                    graph.add_edge(package_node, target_node, kind="runs_on_target")

        for libref, engine in item.get("db_librefs", {}).items():
            libref_node = _libref_id(libref)
            graph.add_node(libref_node, kind="libref", label=libref, name=libref.lower(), engine=engine)
            graph.add_edge(libref_node, file_node, kind="uses_libref")

    for macro_name, spec in manifest.get("macros", {}).items():
        macro_node = _macro_id(macro_name)
        defining_file = spec.get("file")
        graph.add_node(
            macro_node,
            kind="macro",
            label=macro_name,
            name=macro_name.lower(),
            file=defining_file,
            params=spec.get("params", []),
        )
        if defining_file:
            file_node = _file_id(defining_file)
            graph.add_node(file_node, kind="file", label=defining_file, path=defining_file)
            graph.add_edge(file_node, macro_node, kind="defines_macro")

    for item in manifest.get("files", []):
        file_node = _file_id(item["file_path"])
        for macro_name in item.get("called_macros", []):
            macro_node = _macro_id(macro_name)
            if macro_name in manifest.get("macros", {}):
                graph.add_edge(macro_node, file_node, kind="calls_macro")
            else:
                graph.add_node(macro_node, kind="macro", label=macro_name, name=macro_name.lower(), resolved=False)
                graph.add_edge(macro_node, file_node, kind="unresolved_macro_call")

    for function_name, mapping in FUNCTION_MAPPINGS.items():
        function_node = _function_id(function_name)
        package_node = _package_id(mapping.package)
        target_name = _package_to_target(mapping.package)
        target_node = _target_id(target_name)
        graph.add_node(
            function_node,
            kind="function",
            label=function_name,
            name=function_name,
            python_target=mapping.python_target,
            category=mapping.category,
            notes=mapping.notes,
        )
        graph.add_node(package_node, kind="package", label=mapping.package, name=mapping.package.lower())
        graph.add_node(target_node, kind="target", label=target_name, name=target_name.lower())
        graph.add_edge(function_node, package_node, kind="maps_to_package", role="function")
        graph.add_edge(package_node, target_node, kind="runs_on_target")

    return graph


def topological_execution_plan(file_graph: nx.DiGraph) -> dict:
    if nx.is_directed_acyclic_graph(file_graph):
        return {"is_dag": True, "order": list(nx.topological_sort(file_graph))}
    return {"is_dag": False, "cycles": [list(c) for c in nx.simple_cycles(file_graph)]}


def parallel_execution_batches(file_graph: nx.DiGraph) -> dict:
    if not nx.is_directed_acyclic_graph(file_graph):
        return {"is_dag": False, "batches": [], "cycles": [list(c) for c in nx.simple_cycles(file_graph)]}
    batches = [list(batch) for batch in nx.topological_generations(file_graph)]
    return {"is_dag": True, "batch_count": len(batches), "batches": batches}


def migration_graph_insights(migration_graph: nx.DiGraph, file_graph: nx.DiGraph | None = None) -> dict:
    node_counts = Counter(data.get("kind", "unknown") for _, data in migration_graph.nodes(data=True))
    edge_counts = Counter(data.get("kind", "unknown") for _, _, data in migration_graph.edges(data=True))
    degree_rows = []
    for node, degree in migration_graph.degree():
        data = migration_graph.nodes[node]
        degree_rows.append(
            {
                "id": node,
                "kind": data.get("kind"),
                "label": data.get("label", node),
                "degree": degree,
                "in_degree": migration_graph.in_degree(node),
                "out_degree": migration_graph.out_degree(node),
            }
        )
    degree_rows.sort(key=lambda row: (-row["degree"], row["id"]))

    dataset_producers = defaultdict(list)
    dataset_consumers = defaultdict(list)
    for source, target, data in migration_graph.edges(data=True):
        kind = data.get("kind")
        if kind == "writes_dataset":
            dataset_producers[target].append(source)
        elif kind == "reads_dataset":
            dataset_consumers[source].append(target)

    missing_producers = sorted(
        {
            migration_graph.nodes[node].get("label", node)
            for node, consumers in dataset_consumers.items()
            if consumers and not dataset_producers.get(node)
        }
    )
    unused_outputs = sorted(
        {
            migration_graph.nodes[node].get("label", node)
            for node, producers in dataset_producers.items()
            if producers and not dataset_consumers.get(node)
        }
    )

    file_cycles = []
    if file_graph is not None and not nx.is_directed_acyclic_graph(file_graph):
        file_cycles = [list(cycle) for cycle in nx.simple_cycles(file_graph)]

    return {
        "node_counts": dict(sorted(node_counts.items())),
        "edge_counts": dict(sorted(edge_counts.items())),
        "top_degree_nodes": degree_rows[:20],
        "datasets_without_local_producers": missing_producers,
        "datasets_without_downstream_consumers": unused_outputs,
        "file_dependency_cycles": file_cycles,
    }


def impact_report(migration_graph: nx.DiGraph) -> dict:
    rows = []
    for node, data in migration_graph.nodes(data=True):
        kind = data.get("kind")
        if kind not in {"file", "macro", "dataset", "libref"}:
            continue
        descendants = nx.descendants(migration_graph, node)
        affected_files = sorted(
            migration_graph.nodes[desc].get("path", migration_graph.nodes[desc].get("label", desc))
            for desc in descendants
            if migration_graph.nodes[desc].get("kind") == "file"
        )
        if affected_files:
            rows.append(
                {
                    "id": node,
                    "kind": kind,
                    "label": data.get("label", node),
                    "affected_file_count": len(affected_files),
                    "affected_files": affected_files,
                }
            )
    rows.sort(key=lambda row: (-row["affected_file_count"], row["kind"], row["label"]))
    return {"high_impact_nodes": rows}


def graph_to_json(graph: nx.DiGraph) -> dict:
    return {
        "nodes": [{"id": node, **graph.nodes[node]} for node in graph.nodes],
        "edges": [{"source": source, "target": target, **graph.edges[source, target]} for source, target in graph.edges],
    }


def save_json(data: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")