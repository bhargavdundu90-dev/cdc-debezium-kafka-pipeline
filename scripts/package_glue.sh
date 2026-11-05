#!/usr/bin/env bash
# Build dist/cdc_pipeline.zip for the Glue job's --extra-py-files.
# Only the packages the job imports; no tests, generator or docker files.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p dist
rm -f dist/cdc_pipeline.zip
python - <<'PY'
import pathlib, zipfile
root = pathlib.Path(".")
with zipfile.ZipFile("dist/cdc_pipeline.zip", "w", zipfile.ZIP_DEFLATED) as z:
    for pkg in ("ingestion", "transforms", "quality", "pipeline"):
        for f in sorted((root / pkg).rglob("*.py")):
            z.write(f, f.as_posix())
print("dist/cdc_pipeline.zip")
PY
