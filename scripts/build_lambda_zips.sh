#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"
BUILD_DIR="$ROOT_DIR/build"
PLANNER_SRC="$ROOT_DIR/src/lambdas/planner"
WORKER_SRC="$ROOT_DIR/src/lambdas/worker"
WORKER_REQ="$WORKER_SRC/requirements-worker.txt"

echo "==> Cleaning build directory"
rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"

echo "==> Building planner.zip"
mkdir -p "$BUILD_DIR/planner_pkg"
cp "$PLANNER_SRC"/*.py "$BUILD_DIR/planner_pkg/"
(
  cd "$BUILD_DIR/planner_pkg"
  zip -r ../planner.zip .
)

echo "==> Building worker.zip with dependencies"
mkdir -p "$BUILD_DIR/worker_pkg"

python3 -m pip install \
  --upgrade \
  -r "$WORKER_REQ" \
  -t "$BUILD_DIR/worker_pkg"

cp "$WORKER_SRC"/*.py "$BUILD_DIR/worker_pkg/"

(
  cd "$BUILD_DIR/worker_pkg"
  zip -r ../worker.zip .
)

echo "==> Build complete"
ls -lh "$BUILD_DIR"/planner.zip "$BUILD_DIR"/worker.zip