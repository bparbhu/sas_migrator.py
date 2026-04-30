#!/usr/bin/env bash
set -euo pipefail

# Convert a folder of SAS programs into a mirrored Python output folder.
# Run from the repository root:
#   bash examples/convert_folder_example.sh

SOURCE_ROOT="${1:-examples/input_repo}"
OUTPUT_ROOT="${2:-examples/generated_pandas}"
TARGET="${3:-pandas}"

python scripts/convert_sas_folder.py "$SOURCE_ROOT" "$OUTPUT_ROOT" --target "$TARGET" --strict