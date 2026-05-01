from pathlib import Path

from sas_migrator.graph import build_migration_graph
from sas_migrator.graphviz_export import graph_to_dot, write_translation_visualizations


def test_graphviz_export_writes_dot_artifacts(tmp_path: Path):
    manifest = {
        "files": [
            {
                "file_path": "job.sas",
                "includes": [],
                "db_librefs": {},
                "called_macros": [],
                "datasets_read": ["work.have"],
                "datasets_written": ["work.want"],
                "procs_used": ["sql", "logistic"],
            }
        ],
        "macros": {},
    }
    migration_graph = build_migration_graph(manifest)

    dot = graph_to_dot(migration_graph, name="migration_graph")
    assert "proc:logistic" in dot.source
    assert "package:spark ml" in dot.source
    assert "target:pyspark" in dot.source

    artifacts = write_translation_visualizations(tmp_path, migration_graph, migration_graph, migration_graph)
    assert (tmp_path / "graphviz" / "migration_graph.dot").exists()
    assert (tmp_path / "graphviz" / "migration_graph.svg").exists()
    assert artifacts["migration_graph"]["dot"].endswith("migration_graph.dot")
    assert artifacts["migration_graph"]["svg"].endswith("migration_graph.svg")
    assert "rendered" in artifacts["migration_graph"]


def test_graphviz_export_can_write_svg_only_delivery_artifacts(tmp_path: Path):
    manifest = {
        "files": [
            {
                "file_path": "job.sas",
                "includes": [],
                "db_librefs": {},
                "called_macros": [],
                "datasets_read": ["work.have"],
                "datasets_written": ["work.want"],
                "procs_used": ["sql"],
            }
        ],
        "macros": {},
    }
    migration_graph = build_migration_graph(manifest)
    artifacts = write_translation_visualizations(
        tmp_path,
        migration_graph,
        migration_graph,
        migration_graph,
        include_dot=False,
    )
    assert (tmp_path / "graphviz" / "migration_graph.svg").exists()
    assert not (tmp_path / "graphviz" / "migration_graph.dot").exists()
    assert artifacts["migration_graph"]["dot"] is None
