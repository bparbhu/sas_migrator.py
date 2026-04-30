from __future__ import annotations

from pathlib import Path


def build_migration_readiness(repo_root: Path, manifest: dict, strict: bool) -> dict:
    repo_root = repo_root.resolve()
    proc_inventory = sorted(
        {
            proc
            for item in manifest.get("files", [])
            for proc in item.get("procs_used", [])
        }
    )
    macro_count = len(manifest.get("macros", {}))
    file_count = len(manifest.get("files", []))
    return {
        "standardized_environment": {
            "conda_environment_file": str(repo_root / "environment.yml"),
            "conda_environment_file_exists": (repo_root / "environment.yml").exists(),
            "requirements_file": str(repo_root / "requirements.txt"),
            "requirements_file_exists": (repo_root / "requirements.txt").exists(),
            "package_metadata_file": str(repo_root / "pyproject.toml"),
            "package_metadata_file_exists": (repo_root / "pyproject.toml").exists(),
        },
        "migration_inventory": {
            "sas_file_count": file_count,
            "macro_count": macro_count,
            "unique_procs": proc_inventory,
        },
        "quality_gates": {
            "strict_mode_used": strict,
            "generated_python_syntax_validation": True,
            "per_file_reports": True,
            "ir_artifacts": True,
            "ecosystem_plan": True,
            "optional_saspy_baseline_validation": False,
        },
        "follow_up_workstreams": [
            {
                "name": "performance",
                "reason": "Benchmark high-volume DATA/PROC SQL jobs and decide when pandas should be replaced by SQL pushdown, Polars, Dask, or Spark.",
            },
            {
                "name": "validation",
                "reason": "Compare SAS and Python outputs with row counts, schemas, checksums, aggregates, statistical tolerances, and edge-case fixtures.",
            },
            {
                "name": "workflow_integration",
                "reason": "Connect generated Python jobs into the target scheduler, logging, documentation, and downstream data contracts.",
            },
            {
                "name": "team_enablement",
                "reason": "Use reports and IR artifacts for code review, knowledge transfer, and post-migration ownership.",
            },
            {
                "name": "llm_assisted_review",
                "reason": "Optional: use LLMs for explanation, documentation, test generation, and manual-review acceleration, while keeping deterministic compiler output as the source of truth.",
            },
            {
                "name": "saspy_baseline_validation",
                "reason": "Optional: if a licensed SAS runtime is available, use SASPy to execute original SAS and export baseline outputs for equivalence comparison.",
            },
        ],
    }
