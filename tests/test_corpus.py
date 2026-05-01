import json
from pathlib import Path

from sas_migrator.corpus import analyze_sas_corpus
from sas_migrator.graph import build_migration_graph, graph_to_json


def test_corpus_analyzer_writes_report_and_graph_artifacts(tmp_path: Path):
    report = analyze_sas_corpus(
        Path("examples/input_repo"),
        tmp_path / "corpus_output",
        target="pandas",
        large_file_threshold=10,
    )

    assert report["file_count"] == 5
    assert report["total_line_count"] > 0
    assert report["translation_summary"]["translated_count"] == 5
    assert report["translation_summary"]["failed_count"] == 0
    assert report["proc_counts"]["sql"] == 1
    assert report["large_files"]

    report_path = tmp_path / "corpus_output" / "corpus_report.json"
    assert report_path.exists()
    saved = json.loads(report_path.read_text(encoding="utf-8"))
    assert saved["artifacts"]["migration_graph"].endswith("migration_graph.json")
    assert (tmp_path / "corpus_output" / "pandas" / "migration_graph.json").exists()


def test_migration_graph_encodes_proc_and_function_target_mappings():
    manifest = {
        "root": "example",
        "files": [
            {
                "file_path": "model.sas",
                "includes": [],
                "db_librefs": {},
                "called_macros": [],
                "datasets_read": ["work.have"],
                "datasets_written": ["work.want"],
                "procs_used": ["logistic", "sql"],
            }
        ],
        "macros": {},
    }

    graph_json = graph_to_json(build_migration_graph(manifest))
    node_ids = {node["id"] for node in graph_json["nodes"]}
    edge_kinds = {(edge["source"], edge["target"], edge["kind"]) for edge in graph_json["edges"]}

    assert "package:statsmodels" in node_ids
    assert "package:scikit-learn" in node_ids
    assert "package:pandas" in node_ids
    assert "target:python_ecosystem" in node_ids
    assert "target:pandas" in node_ids
    assert "function:sqrt" in node_ids
    assert ("proc:logistic", "package:statsmodels", "maps_to_package") in edge_kinds
    assert ("package:pandas", "target:pandas", "runs_on_target") in edge_kinds
    assert ("function:sqrt", "package:numpy", "maps_to_package") in edge_kinds