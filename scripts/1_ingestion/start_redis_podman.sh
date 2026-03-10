#!/usr/bin/env bash
set -euo pipefail

container_name="crypto-dwh-redis"
volume_name="redis_data"
image="docker.io/library/redis:8-alpine"

if ! command -v podman >/dev/null 2>&1; then
  echo "Error: podman not found in PATH." >&2
  exit 1
fi

if podman container exists "$container_name"; then
  is_running="$(podman inspect -f '{{.State.Running}}' "$container_name")"
  if [[ "$is_running" == "true" ]]; then
    echo "Redis container is already running: $container_name"
    exit 0
  fi

  podman start "$container_name" >/dev/null
  echo "Started existing Redis container: $container_name"
  exit 0
fi

podman volume exists "$volume_name" || podman volume create "$volume_name" >/dev/null

podman run -d \
  --name "$container_name" \
  --restart unless-stopped \
  -p 6379:6379 \
  -v "$volume_name":/data \
  "$image" \
  redis-server --appendonly yes --save 60 1000 >/dev/null

echo "Started Redis container: $container_name"
