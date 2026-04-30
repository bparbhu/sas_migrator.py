from __future__ import annotations

from sas_migrator.saspy_mapping import SASPY_DATASET_OPTIONS, SASPY_TYPE_MAPPING, saspy_mapping_summary


def test_saspy_mapping_documents_no_reverse_translator():
    summary = saspy_mapping_summary()
    assert summary["has_reverse_sas_to_python_translator"] is False
    assert "Python-to-SAS" in summary["summary"]


def test_saspy_dataset_options_align_with_ir_needs():
    for option in ["where", "keep", "drop", "obs", "firstobs", "format", "encoding"]:
        assert option in SASPY_DATASET_OPTIONS


def test_saspy_type_mapping_includes_date_format_caveat():
    assert SASPY_TYPE_MAPPING["sas_numeric_double"]["pandas"] == "float64"
    assert "formats" in SASPY_TYPE_MAPPING["sas_date_format"]["notes"]
    assert SASPY_TYPE_MAPPING["sas_missing"]["pandas"] == "NaN/NaT/pd.NA"
