#!/usr/bin/env bash
set -euo pipefail

python -m compileall src

echo "Local checks OK"