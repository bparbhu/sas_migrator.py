import argparse
import sys
from pathlib import Path

from .corpus import analyze_sas_corpus
from .databricks_plan import build_databricks_plan
from .data_io import convert_sas_folder_to_parquet
from .graph import (
    build_file_graph,
    build_macro_graph,
    build_migration_graph,
    graph_to_json,
    impact_report,
    migration_graph_insights,
    parallel_execution_batches,
    save_json,
    topological_execution_plan,
)
from .graphviz_export import write_translation_visualizations
from .manifest import build_manifest, save_manifest
from .pipeline import translate_tree
from .proc_registry import build_ecosystem_plan
from .readiness import build_migration_readiness
from .saspy_mapping import saspy_mapping_summary
from .spark_registry import build_spark_plan
from .validation_runner import validate_fixture


def main():
    parser = argparse.ArgumentParser(prog="sas-migrator")
    sub = parser.add_subparsers(dest="command", required=True)

    crawl = sub.add_parser("crawl")
    crawl.add_argument("root")
    crawl.add_argument("--output", required=True)

    plan = sub.add_parser("plan")
    plan.add_argument("root")
    plan.add_argument("--output-dir", required=True)

    ecosystem = sub.add_parser("ecosystem-plan")
    ecosystem.add_argument("root")
    ecosystem.add_argument("--output", required=True)

    readiness = sub.add_parser("readiness")
    readiness.add_argument("root")
    readiness.add_argument("--output", required=True)
    readiness.add_argument("--strict", action="store_true")

    spark = sub.add_parser("spark-plan")
    spark.add_argument("root")
    spark.add_argument("--output", required=True)

    databricks = sub.add_parser("databricks-plan")
    databricks.add_argument("root")
    databricks.add_argument("--output", required=True)

    tree = sub.add_parser("translate-tree")
    tree.add_argument("source_root")
    tree.add_argument("output_root")
    tree.add_argument("--target", choices=["pandas", "pyspark", "databricks"], default="pandas")
    tree.add_argument("--strict", action="store_true", help="Return a non-zero exit code when unsupported items are found.")
    tree.add_argument(
        "--audit-artifacts",
        action="store_true",
        help="Also write manifests, JSON reports, expanded SAS, IR, and DOT files for engineering review.",
    )

    validate = sub.add_parser("validate-fixture")
    validate.add_argument("source_root")
    validate.add_argument("--reference", required=True)
    validate.add_argument("--output-dir", required=True)
    validate.add_argument("--report", required=True)
    validate.add_argument("--target", choices=["pandas", "pyspark", "databricks"], default="pandas")
    validate.add_argument("--strict", action="store_true")

    ingest = sub.add_parser("ingest-sas-data")
    ingest.add_argument("source_root")
    ingest.add_argument("output_root")

    saspy = sub.add_parser("saspy-lessons")
    saspy.add_argument("--output", required=True)

    corpus = sub.add_parser("corpus-test")
    corpus.add_argument("source_root")
    corpus.add_argument("--output-dir", required=True)
    corpus.add_argument("--target", choices=["pandas", "pyspark", "databricks"], default="pandas")
    corpus.add_argument("--strict", action="store_true")
    corpus.add_argument("--large-file-threshold", type=int, default=1000)
    corpus.add_argument("--fail-on-errors", action="store_true")

    args = parser.parse_args()
    if args.command == "crawl":
        manifest = build_manifest(Path(args.root))
        save_manifest(manifest, Path(args.output))
        print(f"Saved manifest to {args.output}")
    elif args.command == "plan":
        root = Path(args.root)
        out = Path(args.output_dir)
        out.mkdir(parents=True, exist_ok=True)
        manifest = build_manifest(root)
        file_graph = build_file_graph(manifest)
        macro_graph = build_macro_graph(manifest)
        migration_graph = build_migration_graph(manifest)
        save_json(graph_to_json(file_graph), out / "file_graph.json")
        save_json(graph_to_json(macro_graph), out / "macro_graph.json")
        save_json(graph_to_json(migration_graph), out / "migration_graph.json")
        save_json(topological_execution_plan(file_graph), out / "execution_plan.json")
        save_json(parallel_execution_batches(file_graph), out / "parallel_batches.json")
        save_json(migration_graph_insights(migration_graph, file_graph), out / "graph_insights.json")
        save_json(impact_report(migration_graph), out / "impact_report.json")
        save_json(write_translation_visualizations(out, file_graph, macro_graph, migration_graph), out / "graphviz_artifacts.json")
        save_json(build_ecosystem_plan(manifest), out / "ecosystem_plan.json")
        print(f"Saved planning artifacts to {out}")
    elif args.command == "ecosystem-plan":
        manifest = build_manifest(Path(args.root))
        save_json(build_ecosystem_plan(manifest), Path(args.output))
        print(f"Saved ecosystem plan to {args.output}")
    elif args.command == "readiness":
        manifest = build_manifest(Path(args.root))
        save_json(build_migration_readiness(Path.cwd(), manifest, args.strict), Path(args.output))
        print(f"Saved migration readiness report to {args.output}")
    elif args.command == "spark-plan":
        manifest = build_manifest(Path(args.root))
        save_json(build_spark_plan(manifest), Path(args.output))
        print(f"Saved PySpark plan to {args.output}")
    elif args.command == "databricks-plan":
        manifest = build_manifest(Path(args.root))
        save_json(build_databricks_plan(manifest), Path(args.output))
        print(f"Saved Databricks plan to {args.output}")
    elif args.command == "translate-tree":
        summary = translate_tree(
            Path(args.source_root),
            Path(args.output_root),
            strict=args.strict,
            target=args.target,
            audit_artifacts=args.audit_artifacts,
        )
        print(f'Translated {summary["translated_count"]} files to {summary["target"]}')
        print(f'Unsupported items: {summary["total_unsupported"]}')
        print(f'Graphviz SVGs: {Path(args.output_root) / "graphviz"}')
        if summary["failed_count"]:
            print(f'Failed {summary["failed_count"]} files')
            sys.exit(1)
    elif args.command == "validate-fixture":
        report = validate_fixture(
            Path(args.source_root),
            Path(args.output_dir),
            Path(args.reference),
            Path(args.report),
            target=args.target,
            strict=args.strict,
        )
        print(f'Saved validation report to {args.report}')
        if not report["passed"]:
            sys.exit(1)
    elif args.command == "ingest-sas-data":
        summary = convert_sas_folder_to_parquet(Path(args.source_root), Path(args.output_root))
        print(f'Converted {summary["converted_count"]} SAS data files')
        if summary["failed_count"]:
            print(f'Failed {summary["failed_count"]} SAS data files')
            sys.exit(1)
    elif args.command == "saspy-lessons":
        save_json(saspy_mapping_summary(), Path(args.output))
        print(f"Saved SASPy mapping summary to {args.output}")
    elif args.command == "corpus-test":
        report = analyze_sas_corpus(
            Path(args.source_root),
            Path(args.output_dir),
            target=args.target,
            strict=args.strict,
            large_file_threshold=args.large_file_threshold,
        )
        print(f'Analyzed {report["file_count"]} SAS files ({report["total_line_count"]} lines)')
        print(f'Translated {report["translation_summary"]["translated_count"]} files to {report["target"]}')
        print(f'Unsupported items: {report["translation_summary"]["total_unsupported"]}')
        print(f'Corpus report: {Path(args.output_dir) / "corpus_report.json"}')
        if args.fail_on_errors and report["translation_summary"]["failed_count"]:
            sys.exit(1)


if __name__ == "__main__":
    main()
