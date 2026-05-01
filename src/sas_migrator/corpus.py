from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

from .crawler import find_sas_files
from .manifest import build_manifest
from .pipeline import translate_tree
from .source_io import read_sas_text
from .proc_registry import build_ecosystem_plan


def _line_count(path: Path) -> int:
    return len(read_sas_text(path).splitlines())


def analyze_sas_corpus(
    source_root: Path,
    output_root: Path,
    target: str = "pandas",
    strict: bool = False,
    large_file_threshold: int = 1000,
) -> dict:
    """Translate a SAS corpus and write a high-level migration readiness report."""
    source_root = source_root.resolve()
    output_root = output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    sas_files = find_sas_files(source_root)
    file_rows = []
    total_lines = 0
    for path in sas_files:
        lines = _line_count(path)
        total_lines += lines
        file_rows.append(
            {
                "file": str(path.relative_to(source_root)),
                "line_count": lines,
                "large_file": lines >= large_file_threshold,
            }
        )

    translation_output = output_root / target
    summary = translate_tree(source_root, translation_output, strict=strict, target=target, audit_artifacts=True)
    manifest = build_manifest(source_root)
    ecosystem_plan = build_ecosystem_plan(manifest)

    unsupported_by_code = Counter()
    warning_by_code = Counter()
    generated_reports = sorted(translation_output.rglob("*.report.json"))
    for report_path in generated_reports:
        report = json.loads(report_path.read_text(encoding="utf-8"))
        for issue in report.get("translation", {}).get("issues", []):
            if issue.get("severity") == "warning":
                warning_by_code[issue.get("code", "unknown")] += 1
            if "unsupported" in issue.get("code", ""):
                unsupported_by_code[issue.get("code", "unknown")] += 1

    proc_counts = Counter()
    for item in manifest.get("files", []):
        proc_counts.update(item.get("procs_used", []))

    report = {
        "source_root": str(source_root),
        "target": target,
        "file_count": len(sas_files),
        "total_line_count": total_lines,
        "large_file_threshold": large_file_threshold,
        "large_files": [row for row in file_rows if row["large_file"]],
        "line_count_top_files": sorted(file_rows, key=lambda row: row["line_count"], reverse=True)[:25],
        "proc_counts": dict(sorted(proc_counts.items())),
        "ecosystem_plan": ecosystem_plan,
        "translation_summary": summary,
        "unsupported_issue_counts": dict(sorted(unsupported_by_code.items())),
        "warning_issue_counts": dict(sorted(warning_by_code.items())),
        "artifacts": {
            "translation_output": str(translation_output),
            "manifest": str(translation_output / "manifest.json"),
            "migration_graph": str(translation_output / "migration_graph.json"),
            "graph_insights": str(translation_output / "graph_insights.json"),
            "impact_report": str(translation_output / "impact_report.json"),
            "parallel_batches": str(translation_output / "parallel_batches.json"),
        },
    }
    (output_root / "corpus_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report
