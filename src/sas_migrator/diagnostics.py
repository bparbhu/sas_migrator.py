from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Literal

Severity = Literal["info", "warning", "error"]


@dataclass(frozen=True)
class TranslationIssue:
    severity: Severity
    code: str
    message: str
    block_type: str | None = None
    line: int | None = None


@dataclass
class TranslationReport:
    issues: list[TranslationIssue] = field(default_factory=list)
    blocks_seen: int = 0
    blocks_translated: int = 0
    syntax_valid: bool = False

    @property
    def unsupported_count(self) -> int:
        return sum(1 for issue in self.issues if issue.code.startswith("unsupported"))

    @property
    def error_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == "error")

    @property
    def warning_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == "warning")

    def add(
        self,
        severity: Severity,
        code: str,
        message: str,
        block_type: str | None = None,
        line: int | None = None,
    ) -> None:
        self.issues.append(
            TranslationIssue(
                severity=severity,
                code=code,
                message=message,
                block_type=block_type,
                line=line,
            )
        )

    def to_dict(self) -> dict:
        return {
            "blocks_seen": self.blocks_seen,
            "blocks_translated": self.blocks_translated,
            "syntax_valid": self.syntax_valid,
            "unsupported_count": self.unsupported_count,
            "warning_count": self.warning_count,
            "error_count": self.error_count,
            "issues": [asdict(issue) for issue in self.issues],
        }


@dataclass(frozen=True)
class TranslationResult:
    code: str
    report: TranslationReport
