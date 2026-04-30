from pathlib import Path
import json
from .crawler import find_sas_files
from .databricks_emitter import emit_databricks
from .databricks_plan import build_databricks_plan
from .manifest import build_manifest, save_manifest
from .graph import build_file_graph, build_macro_graph, topological_execution_plan, graph_to_json, save_json
from .bundler import build_bundle
from .macro_engine import expand
from .proc_registry import build_ecosystem_plan
from .pyspark_emitter import emit_pyspark
from .readiness import build_migration_readiness
from .sas_parser import parse_sas_to_ir
from .spark_registry import build_spark_plan
from .translator import translate_with_report

def translate_tree(source_root: Path, output_root: Path, strict: bool = False, target: str = "pandas") -> dict:
    if target not in {"pandas", "pyspark", "databricks"}:
        raise ValueError("target must be 'pandas', 'pyspark', or 'databricks'")
    source_root = source_root.resolve()
    output_root = output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    manifest = build_manifest(source_root)
    save_manifest(manifest, output_root / "manifest.json")

    file_graph = build_file_graph(manifest)
    macro_graph = build_macro_graph(manifest)
    save_json(graph_to_json(file_graph), output_root / "file_graph.json")
    save_json(graph_to_json(macro_graph), output_root / "macro_graph.json")
    save_json(topological_execution_plan(file_graph), output_root / "execution_plan.json")
    save_json(build_ecosystem_plan(manifest), output_root / "ecosystem_plan.json")
    save_json(build_spark_plan(manifest), output_root / "pyspark_plan.json")
    save_json(build_databricks_plan(manifest), output_root / "databricks_plan.json")
    save_json(build_migration_readiness(Path.cwd(), manifest, strict), output_root / "migration_readiness.json")

    translated, failed, files_with_issues = [], [], []
    total_unsupported = 0
    total_warnings = 0
    total_errors = 0
    for src_path in find_sas_files(source_root):
        rel = src_path.relative_to(source_root)
        target_dir = output_root / rel.parent
        target_dir.mkdir(parents=True, exist_ok=True)
        try:
            bundle = build_bundle(source_root, manifest, str(rel))
            expanded, unsupported = expand(bundle["expanded_code"], manifest, bundle.get("let_vars"))
            ir = parse_sas_to_ir(expanded)
            if target == "pyspark":
                result = emit_pyspark(ir)
            elif target == "databricks":
                result = emit_databricks(ir)
            else:
                result = translate_with_report(expanded, bundle["db_librefs"])
            py_code = result.code
            (target_dir / f"{rel.stem}.py").write_text(py_code, encoding="utf-8")
            (target_dir / f"{rel.stem}.expanded.sas").write_text(expanded, encoding="utf-8")
            (target_dir / f"{rel.stem}.ir.json").write_text(json.dumps(ir.to_dict(), indent=2), encoding="utf-8")
            (target_dir / f"{rel.stem}.report.json").write_text(json.dumps({
                "source": str(rel),
                "db_librefs": bundle["db_librefs"],
                "unsupported_macro_items": unsupported,
                "ir_nodes": len(ir.nodes),
                "target": target,
                "translation": result.report.to_dict(),
            }, indent=2), encoding="utf-8")
            translated.append(str(rel))
            total_unsupported += result.report.unsupported_count + len(unsupported)
            total_warnings += result.report.warning_count
            total_errors += result.report.error_count
            actionable_issues = [issue for issue in result.report.issues if issue.severity != "info"]
            if unsupported or actionable_issues:
                files_with_issues.append(str(rel))
            if strict and (unsupported or result.report.unsupported_count or result.report.error_count):
                failed.append({
                    "file": str(rel),
                    "error": "strict quality gate failed",
                    "unsupported_macro_items": unsupported,
                    "translation": result.report.to_dict(),
                })
        except Exception as exc:
            failed.append({"file": str(rel), "error": str(exc)})

    summary = {
        "translated_count": len(translated),
        "failed_count": len(failed),
        "translated_files": translated,
        "failed_files": failed,
        "files_with_issues": files_with_issues,
        "total_unsupported": total_unsupported,
        "total_warnings": total_warnings,
        "total_errors": total_errors,
        "strict": strict,
        "target": target,
    }
    (output_root / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary
