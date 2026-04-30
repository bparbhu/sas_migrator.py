from pathlib import Path

def find_sas_files(root: Path) -> list[Path]:
    return sorted(
        p for p in root.rglob("*")
        if p.is_file() and p.suffix.lower() in {".sas", ".inc"}
    )
