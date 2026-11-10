#!/usr/bin/env bash
# Usage: create_bucket.sh <project> <bucket>
# Creates the bucket only if it does not already exist.
set -euo pipefail
gsutil ls -p "$1" -b "gs://$2" || gsutil mb -p "$1" "gs://$2"
