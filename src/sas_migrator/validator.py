from __future__ import annotations

import ast

from .diagnostics import TranslationReport


def validate_python(code: str, report: TranslationReport) -> bool:
    try:
        ast.parse(code)
    except SyntaxError as exc:
        report.syntax_valid = False
        report.add(
            "error",
            "python_syntax_error",
            exc.msg,
            line=exc.lineno,
        )
        return False
    report.syntax_valid = True
    return True
