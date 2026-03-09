#!/bin/sh
# Clean shutdown: stop dynamically-created consumer containers, then compose down.
set -e

echo "Stopping consumer containers..."
docker ps -aq --filter "name=nats-consumer" | xargs -r docker rm -f 2>/dev/null || true

echo "Running docker compose down..."
docker compose down "$@"
