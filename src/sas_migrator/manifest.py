from __future__ import annotations
import json
import re
from pathlib import Path
from .crawler import find_sas_files

DB_ENGINES = {"odbc","oledb","oracle","sqlsvr","postgres","postgresql","teradata","db2","snowflake","redshift","mysql","sqlite"}
SAS_COMMENT_BLOCK = re.compile(r"/\*.*?\*/", re.DOTALL)
SAS_COMMENT_LINE = re.compile(r"^\s*\*.*?;\s*$", re.MULTILINE)
INCLUDE_RE = re.compile(r'%include\s+[\'"]([^\'"]+)[\'"]\s*;', re.IGNORECASE)
LIBNAME_DB_RE = re.compile(r'libname\s+([A-Za-z_][A-Za-z0-9_]*)\s+([A-Za-z_][A-Za-z0-9_]*)\s+(.*?)\s*;', re.IGNORECASE)
MACRO_DEF_RE = re.compile(r'%macro\s+([A-Za-z_][A-Za-z0-9_]*)(?:\((.*?)\))?\s*;(.*?)%mend\s*(?:\1)?\s*;', re.IGNORECASE | re.DOTALL)
MACRO_CALL_RE = re.compile(r'%([A-Za-z_][A-Za-z0-9_]*)\s*(?:\((.*?)\))?\s*;', re.IGNORECASE)
DATA_STEP_RE = re.compile(r'\bdata\s+([A-Za-z_][A-Za-z0-9_.]*)\s*;', re.IGNORECASE)
SET_RE = re.compile(r'\bset\s+([A-Za-z_][A-Za-z0-9_.]*)', re.IGNORECASE)
MERGE_RE = re.compile(r'\bmerge\s+(.+?);', re.IGNORECASE)
PROC_RE = re.compile(r'\bproc\s+([A-Za-z_][A-Za-z0-9_]*)', re.IGNORECASE)
CREATE_TABLE_RE = re.compile(r'\bcreate\s+table\s+([A-Za-z_][A-Za-z0-9_.]*)', re.IGNORECASE)
FROM_RE = re.compile(r'\bfrom\s+([A-Za-z_][A-Za-z0-9_.]*)', re.IGNORECASE)
LET_RE = re.compile(r'%let\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*?)\s*;', re.IGNORECASE)

def strip_comments(text: str) -> str:
    text = SAS_COMMENT_BLOCK.sub("", text)
    text = SAS_COMMENT_LINE.sub("", text)
    return text

def build_manifest(root: Path) -> dict:
    files = []
    macros = {}
    global_lets = {}
    for path in find_sas_files(root):
        text = strip_comments(path.read_text(encoding="utf-8", errors="ignore"))
        includes = INCLUDE_RE.findall(text)
        db_librefs = {}
        for m in LIBNAME_DB_RE.finditer(text):
            libref, engine = m.group(1).lower(), m.group(2).lower()
            if engine in DB_ENGINES:
                db_librefs[libref] = engine
        file_lets = {}
        for m in LET_RE.finditer(text):
            file_lets[m.group(1).lower()] = m.group(2).strip()
            global_lets[m.group(1).lower()] = m.group(2).strip()
        for m in MACRO_DEF_RE.finditer(text):
            macros[m.group(1).lower()] = {
                "params": [p.strip().split("=")[0].strip() for p in (m.group(2) or "").split(",") if p.strip()],
                "body": m.group(3).strip(),
                "file": str(path),
            }
        called_macros = []
        for m in MACRO_CALL_RE.finditer(text):
            name = m.group(1).lower()
            if name not in {"include", "macro", "mend", "let", "if", "do", "end", "then", "else"}:
                called_macros.append(name)
        datasets_written = [m.group(1) for m in DATA_STEP_RE.finditer(text)]
        datasets_written += [m.group(1) for m in CREATE_TABLE_RE.finditer(text)]
        datasets_read = [m.group(1) for m in SET_RE.finditer(text)]
        for m in MERGE_RE.finditer(text):
            datasets_read.extend(re.findall(r'([A-Za-z_][A-Za-z0-9_.]*)', m.group(1)))
        datasets_read += [m.group(1) for m in FROM_RE.finditer(text)]
        files.append({
            "file_path": str(path),
            "includes": includes,
            "db_librefs": db_librefs,
            "called_macros": sorted(set(called_macros)),
            "datasets_read": sorted(set(datasets_read)),
            "datasets_written": sorted(set(datasets_written)),
            "procs_used": sorted(set(x.group(1).lower() for x in PROC_RE.finditer(text))),
            "let_vars": file_lets,
        })
    return {"root": str(root), "files": files, "macros": macros, "global_lets": global_lets}

def save_manifest(manifest: dict, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
