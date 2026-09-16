#!/bin/sh
# TODO verify that this works with GraphDB agnostic RDF-STORE setup
set -e

# Populate /opt/graphdb/home from the baked-in seed if files don't already exist.
# -n is non-clobbering: bind-mounts (prod) keep their existing files; fresh
# tmpfs mounts (test) get the seed config so userRepo is available on boot.
cp -rn /opt/graphdb-seed/. /opt/graphdb/home/ 2>/dev/null || true

exec /opt/graphdb/dist/bin/graphdb "$@"
