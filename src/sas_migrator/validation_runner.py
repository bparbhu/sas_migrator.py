from __future__ import annotations

import json
from pathlib import Path

from .equivalence import compare_dataframes, execute_python_file, load_reference_module
from .pipeline import translate_tree


def validate_fixture(
    source_root: Path,
    output_root: Path,
    reference_module_path: Path,
    report_path: Path,
    target: str = "pandas",
    strict: bool = True,
) -> dict:
    summary = translate_tree(source_root, output_root, target=target, strict=strict, audit_artifacts=True)
    generated_files = sorted((output_root / "jobs").glob("*.py"))
    compile_errors = []
    for path in generated_files:
        try:
            compile(path.read_text(encoding="utf-8"), str(path), "exec")
        except SyntaxError as exc:
            compile_errors.append({"file": str(path), "line": exc.lineno, "message": exc.msg})

    comparisons = []
    if target == "pandas" and not compile_errors:
        reference = load_reference_module(reference_module_path)
        namespace = reference.input_tables()
        for path in generated_files:
            execute_python_file(path, namespace)
        expected_outputs = reference.expected_outputs()
        sort_keys = getattr(reference, "sort_keys", lambda: {})()
        comparisons = [
            compare_dataframes(name, namespace[name], expected, sort_by=sort_keys.get(name))
            for name, expected in expected_outputs.items()
        ]

    report = {
        "passed": summary["failed_count"] == 0
        and not compile_errors
        and (target != "pandas" or all(item.passed for item in comparisons)),
        "target": target,
        "translation_summary": summary,
        "generated_file_count": len(generated_files),
        "compile_errors": compile_errors,
        "comparisons": [item.__dict__ for item in comparisons],
        "note": "pandas target executes generated code and compares dataframes; pyspark/databricks currently perform generation and syntax validation unless a Spark integration test is supplied.",
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report
