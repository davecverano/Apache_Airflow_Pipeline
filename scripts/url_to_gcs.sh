#!/usr/bin/env bash
# Usage: url_to_gcs.sh <url> <bucket>
# Streams a file from a URL straight into the bucket.
set -euo pipefail
curl -fL "$1" | gsutil cp - "gs://$2/dags/data/in_data.csv"
