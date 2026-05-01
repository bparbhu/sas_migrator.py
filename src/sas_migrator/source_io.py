from __future__ import annotations

from pathlib import Path


def read_sas_text(path: Path) -> str:
    """Read SAS source defensively across common encodings and null-byte artifacts."""
    raw = path.read_bytes()
    if raw.startswith(b"\xff\xfe") or raw.startswith(b"\xfe\xff"):
        return raw.decode("utf-16", errors="ignore").replace("\x00", "")
    text = raw.decode("utf-8-sig", errors="ignore")
    if "\x00" in text:
        without_nulls = text.replace("\x00", "")
        if without_nulls.strip():
            return without_nulls
        return raw.decode("utf-16", errors="ignore").replace("\x00", "")
    return text