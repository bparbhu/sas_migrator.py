from pathlib import Path

from .source_io import read_sas_text

def build_bundle(root: Path, manifest: dict, entry_rel: str) -> dict:
    entry = (root / entry_rel).resolve()
    files_by_path = {Path(f["file_path"]).resolve(): f for f in manifest["files"]}
    visited, code_parts, db_librefs, let_vars = set(), [], {}, dict(manifest.get("global_lets", {}))

    def visit(path: Path):
        if path in visited or path not in files_by_path:
            return
        visited.add(path)
        meta = files_by_path[path]
        for inc in meta.get("includes", []):
            inc_path = (path.parent / inc).resolve()
            if inc_path.exists():
                visit(inc_path)
        db_librefs.update(meta.get("db_librefs", {}))
        let_vars.update(meta.get("let_vars", {}))
        code_parts.append(read_sas_text(path))

    visit(entry)
    return {
        "entrypoint": str(entry),
        "expanded_code": "\n".join(code_parts),
        "db_librefs": db_librefs,
        "let_vars": let_vars,
    }
