#!/usr/bin/env bash
# Clear all triples from the GraphDB repository while preserving everything
# the stack needs to start clean next time: the repository config (config.ttl),
# global settings (settings.js, users.js), and the repository itself.
#
# Default mode hits the SPARQL update endpoint with DROP ALL — the safest way
# to empty a repository while it's running. Use --offline when the stack is
# down: it removes the on-disk index files under repositories/<repo>/storage
# but keeps config.ttl, settings.js, users.js, and the conf dir intact.
#
# Usage:
#   scripts/clear-graphdb.sh            # online: DROP ALL via REST
#   scripts/clear-graphdb.sh --offline  # offline: wipe storage indexes on disk
#   scripts/clear-graphdb.sh --help

set -euo pipefail

REPO_ROOT="$(cd -- "$(dirname -- "$0")/.." && pwd)"
GDB_URL="${FLYOVER_RDF_STORE_URL:-http://localhost:7200}"
REPO="${FLYOVER_REPOSITORY_NAME:-userRepo}"
DATA_DIR="${REPO_ROOT}/graphdb/data"

usage() {
  cat <<EOF
Clear GraphDB data while preserving config.

  Online  (default): DROP ALL via the running GraphDB at \$FLYOVER_RDF_STORE_URL
  Offline (--offline): delete on-disk storage indexes for the repository

Env:
  FLYOVER_RDF_STORE_URL   default http://localhost:7200
  FLYOVER_REPOSITORY_NAME default userRepo

The following are NEVER touched:
  graphdb/data/data/settings.js
  graphdb/data/data/users.js
  graphdb/data/data/repositories/<repo>/config.ttl
  graphdb/data/conf/
EOF
}

case "${1:-}" in
  -h | --help)
    usage
    exit 0
    ;;
esac

if [ "${1:-}" = "--offline" ]; then
  storage_dir="${DATA_DIR}/data/repositories/${REPO}/storage"
  config_file="${DATA_DIR}/data/repositories/${REPO}/config.ttl"

  if [ ! -d "${storage_dir}" ]; then
    echo "Offline clear: nothing to do (no storage at ${storage_dir})."
    exit 0
  fi
  if [ ! -f "${config_file}" ]; then
    echo "Refusing to touch storage: ${config_file} is missing." >&2
    echo "That file is the repository config; without it the repo can't restart." >&2
    exit 1
  fi
  if curl -fsS --max-time 2 "${GDB_URL}/rest/repositories" >/dev/null 2>&1; then
    echo "Refusing offline clear while GraphDB is running at ${GDB_URL}." >&2
    echo "Use the online mode (omit --offline) or stop the container first." >&2
    exit 1
  fi

  echo "Offline clear: removing ${storage_dir}/* (config.ttl preserved)."
  # Use find rather than rm -rf <dir>/* so dotfiles are caught and an empty
  # storage dir is harmless.
  find "${storage_dir}" -mindepth 1 -delete
  echo "Done. Repository '${REPO}' storage is empty; config.ttl preserved."
  exit 0
fi

echo "Online clear: DROP ALL on '${REPO}' at ${GDB_URL} ..."

if ! curl -fsS --max-time 5 "${GDB_URL}/rest/repositories" >/dev/null 2>&1; then
  echo "Error: GraphDB is not reachable at ${GDB_URL}." >&2
  echo "Start the stack ('docker compose up -d') or rerun with --offline." >&2
  exit 1
fi

if ! curl -fsS "${GDB_URL}/rest/repositories" | grep -q "\"id\"\\s*:\\s*\"${REPO}\""; then
  echo "Error: repository '${REPO}' not found at ${GDB_URL}." >&2
  echo "Set FLYOVER_REPOSITORY_NAME to the correct name and retry." >&2
  exit 1
fi

# DROP ALL clears every named graph and the default graph. The repository,
# its config, indexes-on-disk structure, users, and settings all stay put.
http_code=$(
  curl -sS -o /tmp/clear-graphdb.body -w "%{http_code}" \
    -X POST \
    -H 'Content-Type: application/sparql-update' \
    --data 'DROP ALL' \
    "${GDB_URL}/repositories/${REPO}/statements"
)

if [ "${http_code}" != "204" ] && [ "${http_code}" != "200" ]; then
  echo "DROP ALL failed (HTTP ${http_code}):" >&2
  cat /tmp/clear-graphdb.body >&2 || true
  exit 1
fi

# Sanity-check the count is now zero.
count=$(
  curl -fsS \
    -H 'Accept: text/csv' \
    --data-urlencode 'query=SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }' \
    "${GDB_URL}/repositories/${REPO}" \
    | tail -n 1 | tr -d '\r'
)
echo "Done. Repository '${REPO}' now contains ${count} triple(s)."
