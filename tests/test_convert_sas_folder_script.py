import importlib.util
import json
import sys
from pathlib import Path


def load_convert_script():
    repo_root = Path(__file__).resolve().parents[1]
    script_path = repo_root / "scripts" / "convert_sas_folder.py"
    spec = importlib.util.spec_from_file_location("convert_sas_folder", script_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_convert_sas_folder_script_writes_mirrored_python_tree(tmp_path: Path, monkeypatch, capsys):
    module = load_convert_script()
    output_root = tmp_path / "generated_pandas"

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "convert_sas_folder.py",
            "examples/input_repo",
            str(output_root),
            "--target",
            "pandas",
            "--strict",
        ],
    )

    exit_code = module.main()
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "SAS folder conversion complete" in captured.out
    assert "Translated files: 5" in captured.out
    assert (output_root / "jobs" / "sales" / "job1.py").exists()
    assert (output_root / "jobs" / "ops" / "job2.py").exists()
    assert (output_root / "jobs" / "sql" / "job3.py").exists()
    assert (output_root / "jobs" / "merge" / "job4.py").exists()
    assert (output_root / "macros" / "common.py").exists()
    assert (output_root / "jobs" / "sales" / "job1.report.json").exists()

    summary = json.loads((output_root / "summary.json").read_text(encoding="utf-8"))
    assert summary["target"] == "pandas"
    assert summary["translated_count"] == 5
    assert summary["failed_count"] == 0
    assert summary["total_unsupported"] == 0


def test_convert_sas_folder_script_returns_error_for_missing_source(tmp_path: Path, monkeypatch, capsys):
    module = load_convert_script()
    missing_source = tmp_path / "missing_sas_folder"

    monkeypatch.setattr(
        sys,
        "argv",
        ["convert_sas_folder.py", str(missing_source), str(tmp_path / "out")],
    )

    exit_code = module.main()
    captured = capsys.readouterr()

    assert exit_code == 2
    assert "Source folder does not exist" in captured.err