#!/usr/bin/env sh
set -e

if [ -z "$PREFECT_API_URL" ]; then
  echo "PREFECT_API_URL is not set; exiting."
  exit 1
fi

POOL_NAME="${PREFECT_WORK_POOL:-merchant-intel-pool}"

echo "Using Prefect API at: $PREFECT_API_URL"
echo "Target work pool: $POOL_NAME"

echo "Waiting for Prefect server to be ready..."

until curl -sf "${PREFECT_API_URL%/}/health" >/dev/null 2>&1; do
  echo "Prefect server not ready yet, retrying in 3 seconds..."
  sleep 3
done

echo "Prefect server is up. Starting worker for pool '$POOL_NAME'..."
exec prefect worker start --pool "$POOL_NAME" --with-healthcheck
