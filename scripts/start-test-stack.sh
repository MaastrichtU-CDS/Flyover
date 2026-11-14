#!/bin/sh
# Bring up the Flyover stack with a tmpfs-backed GraphDB (clean state every run)
# and wait until both services are answering requests.
#
# Compose creates every container but starts only Flyover and its store
# launcher; the store starts when a user chooses to work on it. This script
# chooses the standard RDF flow through Flyover's API, as the start-up pop-up
# does, so the store is up before the tests begin.
set -e

REPO_ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$REPO_ROOT"

# The CI test stack always runs against GraphDB. Force the graphdb profile so
# the rdf-store-graphdb service (and its build context) is activated,
# regardless of any COMPOSE_PROFILES value coming from the environment or .env.
export COMPOSE_PROFILES=graphdb

COMPOSE="docker compose -f docker-compose.yml -f docker-compose.test.yml"
FLYOVER=http://localhost:5000

fail() {
  echo "$1" >&2
  $COMPOSE logs --no-color
  exit 1
}

$COMPOSE create --build
$COMPOSE up -d flyover

echo "Waiting for Flyover..."
i=0
until curl -fsS "$FLYOVER/" -o /dev/null 2>&1; do
  i=$((i + 1))
  [ "$i" -ge 60 ] && fail "Flyover failed to answer within 180s"
  sleep 3
done

echo "Choosing the standard RDF flow, which starts the store..."
curl -fsS -X DELETE -H "Origin: $FLYOVER" "$FLYOVER/api/v1/project" -o /dev/null

echo "Waiting for the store..."
i=0
until curl -fsS "$FLYOVER/api/v1/project/store-status" 2>/dev/null \
    | grep -q '"state": *"reachable"'; do
  i=$((i + 1))
  [ "$i" -ge 60 ] && fail "The store failed to answer within 180s"
  sleep 3
done
echo "Stack ready"
