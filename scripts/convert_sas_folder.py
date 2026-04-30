"""Convert a folder of SAS programs into a mirrored folder of Python files.

Default demo:
    python scripts/convert_sas_folder.py

Use your own folders:
    python scripts/convert_sas_folder.py C:\sas_code C:\translated_python --target pandas --strict

Supported targets: pandas, pyspark, databricks.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from sas_migrator.pipeline import translate_tree


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert a folder tree of .sas files into a mirrored Python output tree."
    )
    parser.add_argument(
        "source_root",
        nargs="?",
        default=str(REPO_ROOT / "examples" / "input_repo"),
        help="Folder containing SAS code. Defaults to examples/input_repo.",
    )
    parser.add_argument(
        "output_root",
        nargs="?",
        default=str(REPO_ROOT / "examples" / "generated_pandas"),
        help="Folder where generated Python and audit artifacts will be written.",
    )
    parser.add_argument(
        "--target",
        choices=["pandas", "pyspark", "databricks"],
        default="pandas",
        help="Python runtime target to generate. Defaults to pandas.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero if unsupported SAS constructs are found.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    source_root = Path(args.source_root).expanduser().resolve()
    output_root = Path(args.output_root).expanduser().resolve()

    if not source_root.exists():
        print(f"Source folder does not exist: {source_root}", file=sys.stderr)
        return 2

    summary = translate_tree(source_root, output_root, strict=args.strict, target=args.target)

    print("SAS folder conversion complete")
    print(f"  Source: {source_root}")
    print(f"  Output: {output_root}")
    print(f"  Target: {summary['target']}")
    print(f"  Translated files: {summary['translated_count']}")
    print(f"  Unsupported items: {summary['total_unsupported']}")
    print(f"  Warnings: {summary['total_warnings']}")
    print(f"  Failed files: {summary['failed_count']}")
    print(f"  Summary report: {output_root / 'summary.json'}")

    if summary["failed_count"]:
        print(json.dumps(summary["failed_files"], indent=2))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())