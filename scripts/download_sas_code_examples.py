"""Download the public SAS Software examples repository as a local corpus.

This uses only the Python standard library and downloads the default branch zip.

Example:
    python scripts/download_sas_code_examples.py external_corpora/sas-code-examples
"""

from __future__ import annotations

import argparse
import shutil
import tempfile
import urllib.request
import zipfile
from pathlib import Path

DEFAULT_URL = "https://github.com/sassoftware/sas-code-examples/archive/refs/heads/main.zip"


def main() -> int:
    parser = argparse.ArgumentParser(description="Download sassoftware/sas-code-examples for corpus testing.")
    parser.add_argument("output_dir", nargs="?", default="external_corpora/sas-code-examples")
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    output_dir = Path(args.output_dir).resolve()
    if output_dir.exists() and any(output_dir.iterdir()) and not args.overwrite:
        print(f"Output folder already exists and is not empty: {output_dir}")
        print("Use --overwrite to replace it.")
        return 2

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        zip_path = tmp_path / "sas-code-examples.zip"
        print(f"Downloading {args.url}")
        urllib.request.urlretrieve(args.url, zip_path)
        with zipfile.ZipFile(zip_path) as archive:
            archive.extractall(tmp_path)
        extracted_roots = [path for path in tmp_path.iterdir() if path.is_dir()]
        if not extracted_roots:
            raise RuntimeError("Downloaded archive did not contain a folder.")
        source = extracted_roots[0]
        if output_dir.exists():
            shutil.rmtree(output_dir)
        output_dir.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(source, output_dir)

    print(f"Downloaded SAS examples to {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())