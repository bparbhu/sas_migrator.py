from pathlib import Path
import pandas as pd
from sas_migrator.function_registry import classify_function
from sas_migrator.imports import build_pandas_imports
from sas_migrator.pipeline import translate_tree
from sas_migrator.databricks_emitter import emit_databricks
from sas_migrator.databricks_plan import build_databricks_plan
from sas_migrator.proc_registry import build_ecosystem_plan, classify_proc
from sas_migrator.pyspark_emitter import emit_pyspark
from sas_migrator.runtime import sas_first_last_flags, sas_style_merge
from sas_migrator.sas_parser import parse_sas_to_ir
from sas_migrator.spark_registry import build_spark_plan
from sas_migrator.translator import translate_with_report

def test_translate_tree(tmp_path: Path):
    source = Path("examples/input_repo")
    out = tmp_path / "output"
    summary = translate_tree(source, out)
    assert summary["translated_count"] >= 4
    assert (out / "jobs" / "sales" / "job1.py").exists()
    assert (out / "jobs" / "ops" / "job2.py").exists()
    assert (out / "jobs" / "sql" / "job3.py").exists()
    assert (out / "jobs" / "sql" / "job3.ir.json").exists()
    assert (out / "jobs" / "merge" / "job4.py").exists()
    assert (out / "execution_plan.json").exists()
    assert (out / "ecosystem_plan.json").exists()
    assert (out / "pyspark_plan.json").exists()
    assert (out / "databricks_plan.json").exists()
    assert (out / "migration_readiness.json").exists()
    assert summary["total_errors"] == 0
    assert (out / "jobs" / "sales" / "job1.report.json").exists()

def test_translator_reports_python_syntax_and_single_line_data_step():
    result = translate_with_report("data work.out; set work.in; total = qty * price; run;")
    assert result.report.syntax_valid is True
    assert result.report.error_count == 0
    assert result.report.unsupported_count == 0
    assert 'work_out = work_in.copy()' in result.code
    assert 'work_out["total"] = work_out["qty"] * work_out["price"]' in result.code

def test_translator_reports_unsupported_blocks():
    result = translate_with_report("proc mystery data=x; run;")
    assert result.report.syntax_valid is True
    assert result.report.unsupported_count == 1
    assert result.report.issues[0].code == "unsupported_block"

def test_database_reads_use_runtime_sqlalchemy_helper():
    result = translate_with_report(
        "libname dw oracle; data local; set dw.sales(where=(region = 'East')); run;",
        {"dw": "oracle"},
    )
    assert 'read_database_table("dw", "oracle", "sales", "region = \'East\'")' in result.code

def test_first_last_flags_preserve_sas_tie_order():
    df = pd.DataFrame(
        {
            "patient_id": [101, 101, 101],
            "visit_date": ["2023-01-15", "2023-01-15", "2023-01-20"],
            "value": [10, 20, 30],
        }
    )
    flagged = sas_first_last_flags(df, ["patient_id", "visit_date"])
    last_tied_visit = flagged.loc[flagged["LAST_visit_date"] & flagged["visit_date"].eq("2023-01-15")]
    assert last_tied_visit["value"].tolist() == [20]

def test_sas_style_merge_keeps_in_flag_records_after_outer_merge():
    baseline = pd.DataFrame({"patient_id": [101], "age": [45]})
    treatment = pd.DataFrame({"patient_id": [101, 101], "drug": ["A", "B"]})
    outcomes = pd.DataFrame({"patient_id": [102], "result": ["positive"]})
    merged = sas_style_merge(
        [("a", baseline), ("b", treatment), ("c", outcomes)],
        by=["patient_id"],
        keep_flags=["a"],
    )
    assert merged["patient_id"].tolist() == [101, 101]
    assert merged["drug"].tolist() == ["A", "B"]

def test_data_step_first_last_translation_uses_runtime_flags():
    result = translate_with_report(
        "data last_records; set patient_data; by patient_id visit_date; if last.patient_id; run;"
    )
    assert "sas_first_last_flags" in result.code
    assert 'last_records["LAST_patient_id"]' in result.code

def test_statistical_proc_is_reported_for_validation_triage():
    result = translate_with_report("proc glm data=x; model y = a b; run;")
    assert result.report.issues[0].code == "unsupported_statistical_proc"

def test_retain_counter_pattern_translates_to_sas_runtime_helper():
    result = translate_with_report(
        "data sequence_data; set patient_visits; by patient_id visit_date; "
        "retain visit_count 0; if first.patient_id then visit_count=1; "
        "else visit_count+1; visit_number = visit_count; run;"
    )
    assert 'sas_retain_cumcount(sequence_data, by=[\'patient_id\'], target="visit_count", start=1)' in result.code

def test_sas_date_functions_and_literals_translate_to_runtime_helpers():
    result = translate_with_report(
        "data x; set y; year_var = year(date_var); month_var = month('15JAN2023'D); run;"
    )
    assert 'x["year_var"] = sas_year(x["date_var"])' in result.code
    assert 'x["month_var"] = sas_month(sas_date_literal("15JAN2023"))' in result.code

def test_proc_sql_left_join_wildcard_case_and_sas_missing_value():
    sas = """
    proc sql;
      create table sd1.want as
      select l.*
            ,case when r.bmi = . then 100 else bmi end as bmi
      from sd1.have as l left join sd1.havBMI as r
        on l.name = r.name
      where l.name ne 'Alice'
    ;quit;
    """
    result = translate_with_report(sas)
    assert result.report.unsupported_count == 0
    assert "sd1_want_joined = sd1_have.merge(sd1_havBMI, how='left'" in result.code
    assert "sd1_want = sd1_want_filtered.loc[:, list(sd1_have.columns)].copy()" in result.code
    assert 'sd1_want["bmi"] = sd1_want_filtered["bmi"]' in result.code
    assert 'sd1_want.loc[sd1_want_filtered["bmi"].isna(), "bmi"] = 100' in result.code

def test_parser_builds_ir_for_data_step_and_proc_sql():
    program = parse_sas_to_ir(
        """
        data last_records;
          set patient_data;
          by patient_id visit_date;
          if last.patient_id;
        run;

        proc sql;
          create table work.want as
          select l.*, case when r.bmi = . then 100 else bmi end as bmi
          from sd1.have as l left join sd1.havbmi as r
          on l.name = r.name
          where l.name ne 'Alice';
        quit;
        """
    )
    assert len(program.nodes) == 2
    assert program.nodes[0].kind == "data_step"
    assert program.nodes[0].first_last_filter == ("LAST", "patient_id")
    assert program.nodes[1].kind == "proc_sql"
    assert "case when" in program.nodes[1].select.lower()

def test_proc_registry_routes_modeling_procs_outside_pandas():
    assert classify_proc("glm").primary_package == "statsmodels"
    assert classify_proc("logistic").fallback_package == "scikit-learn"
    assert classify_proc("fastclus").primary_package == "scikit-learn"
    assert classify_proc("arima").primary_package == "statsmodels"

def test_ecosystem_plan_counts_manifest_procs():
    manifest = {
        "files": [
            {"procs_used": ["sql", "glm", "logistic"]},
            {"procs_used": ["sql", "fastclus"]},
        ]
    }
    plan = build_ecosystem_plan(manifest)
    by_proc = {row["proc"]: row for row in plan["mappings"]}
    assert by_proc["sql"]["primary_package"] == "pandas"
    assert by_proc["glm"]["category"] == "statistical_modeling"
    assert by_proc["fastclus"]["primary_package"] == "scikit-learn"

def test_function_registry_routes_numpy_and_scipy_operations():
    assert classify_function("sqrt").python_target == "np.sqrt"
    assert classify_function("sum").python_target == "np.nansum"
    assert classify_function("probnorm").python_target == "scipy.stats.norm.cdf"
    assert classify_function("cdf").package == "scipy"

def test_proc_stdize_reponly_imputation_translation():
    result = translate_with_report(
        """
        proc stdize data=have out=want reponly;
          var age weight;
          repvalue=median;
        run;
        """
    )
    assert result.report.unsupported_count == 0
    assert "want = have.copy()" in result.code
    assert 'want["age"] = want["age"].fillna(want["age"].median())' in result.code
    assert 'want["weight"] = want["weight"].fillna(want["weight"].median())' in result.code

def test_spark_plan_identifies_distributed_candidates():
    plan = build_spark_plan(
        {
            "files": [
                {"procs_used": ["sql", "sort", "freq"]},
                {"procs_used": ["glm"]},
            ]
        }
    )
    candidate_names = {item["proc"] for item in plan["candidate_procs"]}
    assert {"sql", "sort", "freq"} <= candidate_names
    assert "glm" not in candidate_names
    assert any(pattern["pyspark_target"] == "unionByName" for pattern in plan["patterns"])

def test_translate_tree_can_target_pyspark(tmp_path: Path):
    source = Path("examples/input_repo")
    out = tmp_path / "spark_output"
    summary = translate_tree(source, out, target="pyspark")
    assert summary["target"] == "pyspark"
    assert summary["translated_count"] >= 4
    sales_code = (out / "jobs" / "sales" / "job1.py").read_text()
    assert "from pyspark.sql import SparkSession, Window" in sales_code
    assert ".orderBy(" in sales_code
    assert ".groupBy(" in sales_code

def test_pyspark_emitter_handles_basic_data_step():
    program = parse_sas_to_ir("data work.out; set work.in(keep=id qty price where=(qty > 0)); total = qty * price; run;")
    result = emit_pyspark(program)
    assert result.report.syntax_valid is True
    assert result.report.unsupported_count == 0
    assert "work_out = work_in.select(*['id', 'qty', 'price'])" in result.code
    assert 'work_out = work_out.filter(F.expr(\'qty > 0\'))' in result.code
    assert 'work_out = work_out.withColumn("total", F.expr(\'qty * price\'))' in result.code

def test_translate_tree_can_target_databricks(tmp_path: Path):
    source = Path("examples/input_repo")
    out = tmp_path / "databricks_output"
    summary = translate_tree(source, out, target="databricks")
    assert summary["target"] == "databricks"
    code = (out / "jobs" / "sales" / "job1.py").read_text()
    assert "# Databricks notebook source" in code
    assert "dbutils.widgets.text('catalog', '')" in code
    assert "spark.table(\"dw.sales\")" in code

def test_databricks_emitter_wraps_pyspark_code():
    program = parse_sas_to_ir("data work.out; set work.in; x = 1; run;")
    result = emit_databricks(program)
    assert result.report.syntax_valid is True
    assert "# Databricks notebook source" in result.code
    assert "dbutils.widgets.get('catalog')" in result.code
    assert result.report.issues[-1].code == "databricks_target"

def test_databricks_plan_contains_platform_mappings():
    plan = build_databricks_plan({"files": [{"procs_used": ["sql", "logistic"]}]})
    assert plan["target"] == "databricks"
    assert any("Unity Catalog" in item for item in plan["recommended_artifacts"])
    assert any(row["databricks"] == "PySpark DataFrame pipeline" for row in plan["sas_to_databricks_concepts"])

def test_pandas_imports_are_pruned_for_simple_translation():
    result = translate_with_report("data work.out; set work.in; total = qty * price; run;")
    assert result.code.startswith("import pandas as pd\n\n")
    assert "from sas_migrator.runtime import" not in result.code

def test_pandas_imports_include_only_used_runtime_helpers():
    result = translate_with_report(
        "data work.out; set work.in; by id; if last.id; year_var = year(date_var); run;"
    )
    assert "from sas_migrator.runtime import (" in result.code
    assert "    sas_first_last_flags," in result.code
    assert "    sas_year," in result.code
    assert "    sas_style_merge," not in result.code
    assert "    read_database_table," not in result.code

def test_import_manager_detects_runtime_helpers():
    imports = build_pandas_imports(["x = sas_style_merge([])", "y = sas_month(x)"])
    assert imports == [
        "import pandas as pd",
        "from sas_migrator.runtime import (",
        "    sas_month,",
        "    sas_style_merge,",
        ")",
    ]
