#!/bin/sh
# Bring up the Flyover stack with a tmpfs-backed GraphDB (clean state every run)
# and wait until both services are answering requests.
set -e

REPO_ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$REPO_ROOT"

docker compose -f docker-compose.yml -f docker-compose.test.yml up -d --build

echo "Waiting for stack to become ready..."
for i in $(seq 1 60); do
  if curl -fsS http://localhost:7200/rest/repositories 2>/dev/null | grep -q userRepo \
     && curl -fsS http://localhost:5000/app/ -o /dev/null 2>&1; then
    echo "Stack ready after ${i} attempts"
    exit 0
  fi
  sleep 3
done

echo "Stack failed to become ready within 180s" >&2
docker compose -f docker-compose.yml -f docker-compose.test.yml logs --no-color
exit 1
