#!/usr/bin/env bash
set -euo pipefail

: "${ARTIFACT_BUCKET?Set ARTIFACT_BUCKET env var, e.g. market-pulse-artifacts-eu-central-1}"
: "${AWS_REGION:=eu-central-1}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="$ROOT_DIR/build"

GIT_SHA="$(git rev-parse --short HEAD)"
PREFIX="artifacts/lambda/${GIT_SHA}"

aws s3 cp "$BUILD_DIR/planner.zip" "s3://${ARTIFACT_BUCKET}/${PREFIX}/planner.zip" --region "$AWS_REGION"
aws s3 cp "$BUILD_DIR/worker.zip"  "s3://${ARTIFACT_BUCKET}/${PREFIX}/worker.zip"  --region "$AWS_REGION"

echo "Uploaded to s3://${ARTIFACT_BUCKET}/${PREFIX}/"