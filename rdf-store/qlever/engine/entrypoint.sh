#!/usr/bin/env bash
set -euo pipefail

# ─── Configuration ───────────────────────────────────────────────────
INDEX_DIR="/data"
INDEX_BASENAME="${INDEX_DIR}/userRepo"
PORT="${QLEVER_PORT:-7019}"
ACCESS_TOKEN="${QLEVER_ACCESS_TOKEN:-flyover}"
MEMORY_MAX="${QLEVER_MEMORY_MAX:-4G}"
SEED_FILE="/seed/seed.nt"

mkdir -p "${INDEX_DIR}"

# ─── Build the index on first start ──────────────────────────────────
# QLever needs an on-disk index before it can serve queries. We build a
# minimal index from the seed file; the real data is added afterwards
# through SPARQL UPDATE (sent by the Flyover adapter).
if [ ! -f "${INDEX_BASENAME}.meta" ] && [ ! -f "${INDEX_BASENAME}.index.pso" ]; then
    echo "[qlever] No existing index found — building from seed ${SEED_FILE}..."
    # In current QLever images the index builder/server binaries live in
    # /qlever and are named qlever-index / qlever-server (the old
    # IndexBuilderMain / ServerMain names no longer exist). The input-file
    # flag is now --kg-input-file.
    qlever-index \
        --index-basename "${INDEX_BASENAME}" \
        --file-format nt \
        --kg-input-file "${SEED_FILE}"
    echo "[qlever] Index build complete."
else
    echo "[qlever] Existing index found — reusing ${INDEX_BASENAME}."
fi

# ─── Start the server ────────────────────────────────────────────────
# `--persist-updates` is essential here: the real data is loaded exclusively
# through SPARQL UPDATE (via the adapter), not baked into the on-disk index
# (which only holds the seed). Without this flag QLever keeps update results
# in memory only and silently discards them on restart, so all ingested
# named graphs (e.g. http://data.local/...) would vanish and checks such as
# `check-graph-exists` would return false after any engine restart.
echo "[qlever] Starting qlever-server on port ${PORT}..."
exec qlever-server \
    --index-basename "${INDEX_BASENAME}" \
    --port "${PORT}" \
    --access-token "${ACCESS_TOKEN}" \
    --memory-max-size "${MEMORY_MAX}" \
    --persist-updates
