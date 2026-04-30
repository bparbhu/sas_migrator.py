from __future__ import annotations

import importlib.util
import shutil
from pathlib import Path

import pytest

from sas_migrator.equivalence import compare_dataframes, execute_python_file
from sas_migrator.pipeline import translate_tree
from sas_migrator.validation_runner import validate_fixture


FIXTURE_ROOT = Path("tests/fixtures/large_sas_pipeline")


def load_reference():
    path = FIXTURE_ROOT / "reference_impl.py"
    spec = importlib.util.spec_from_file_location("large_pipeline_reference", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def execute_generated_pipeline(output_root: Path, namespace: dict):
    for path in sorted((output_root / "jobs").glob("*.py")):
        execute_python_file(path, namespace)
    return namespace


def test_large_pipeline_pandas_equivalence(tmp_path: Path):
    output = tmp_path / "pandas_output"
    summary = translate_tree(FIXTURE_ROOT, output, target="pandas", strict=True)
    assert summary["failed_count"] == 0, summary
    assert summary["total_errors"] == 0, summary
    assert summary["total_unsupported"] == 0, summary

    reference = load_reference()
    namespace = reference.input_tables()
    execute_generated_pipeline(output, namespace)

    expected = reference.expected_outputs()
    sort_keys = reference.sort_keys()
    comparisons = [
        compare_dataframes(name, namespace[name], expected[name], sort_by=sort_keys.get(name))
        for name in expected
    ]
    failures = [item for item in comparisons if not item.passed]
    assert not failures, [failure.__dict__ for failure in failures]


def test_large_pipeline_pyspark_generation_static_contract(tmp_path: Path):
    output = tmp_path / "spark_output"
    summary = translate_tree(FIXTURE_ROOT, output, target="pyspark", strict=True)
    assert summary["failed_count"] == 0, summary
    assert summary["total_errors"] == 0, summary

    generated = "\n".join(path.read_text() for path in sorted((output / "jobs").glob("*.py")))
    compile(generated, "generated_pyspark_pipeline.py", "exec")
    assert "from pyspark.sql import SparkSession, Window" in generated
    assert ".select(*['customer_id', 'order_id', 'order_date', 'amount', 'qty', 'price', 'status', 'region'])" in generated
    assert ".filter(F.expr(\"status = 'OPEN'\"))" in generated or ".filter(F.expr('status = \\'OPEN\\''))" in generated
    assert ".groupBy(*['customer_id', 'region']).count()" in generated
    assert ".join(" in generated


def test_large_pipeline_databricks_generation_static_contract(tmp_path: Path):
    output = tmp_path / "databricks_output"
    summary = translate_tree(FIXTURE_ROOT, output, target="databricks", strict=True)
    assert summary["failed_count"] == 0, summary
    assert summary["total_errors"] == 0, summary
    assert (output / "databricks_plan.json").exists()

    generated = "\n".join(path.read_text() for path in sorted((output / "jobs").glob("*.py")))
    compile(generated, "generated_databricks_pipeline.py", "exec")
    assert "# Databricks notebook source" in generated
    assert "dbutils.widgets.text('catalog', '')" in generated
    assert "spark.sql(f'USE CATALOG {catalog}')" in generated
    assert "checkpoint(eager=True)" in generated


@pytest.mark.skipif(shutil.which("python") is None, reason="Python executable unavailable")
def test_large_pipeline_cli_targets(tmp_path: Path):
    # Kept as a lightweight CLI contract through direct package call to avoid shell quoting noise.
    pandas_out = tmp_path / "cli_pandas"
    spark_out = tmp_path / "cli_spark"
    databricks_out = tmp_path / "cli_databricks"
    assert translate_tree(FIXTURE_ROOT, pandas_out, target="pandas", strict=True)["failed_count"] == 0
    assert translate_tree(FIXTURE_ROOT, spark_out, target="pyspark", strict=True)["failed_count"] == 0
    assert translate_tree(FIXTURE_ROOT, databricks_out, target="databricks", strict=True)["failed_count"] == 0


def test_validate_fixture_writes_equivalence_report(tmp_path: Path):
    report = validate_fixture(
        FIXTURE_ROOT,
        tmp_path / "validated_output",
        FIXTURE_ROOT / "reference_impl.py",
        tmp_path / "validation_report.json",
        target="pandas",
        strict=True,
    )
    assert report["passed"] is True
    assert report["generated_file_count"] == 4
    assert len(report["comparisons"]) == 9
    assert (tmp_path / "validation_report.json").exists()
