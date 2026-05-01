from pathlib import Path

from sas_migrator.source_io import read_sas_text


def test_read_sas_text_strips_null_bytes(tmp_path: Path):
    path = tmp_path / "sample.sas"
    path.write_bytes(b"d\x00a\x00t\x00a\x00 work.x;\x00 r\x00u\x00n\x00;\x00")

    text = read_sas_text(path)

    assert "\x00" not in text
    assert "data work.x" in text


def test_read_sas_text_handles_utf16_bom(tmp_path: Path):
    path = tmp_path / "sample_utf16.sas"
    path.write_text("data work.x; run;", encoding="utf-16")

    text = read_sas_text(path)

    assert text == "data work.x; run;"