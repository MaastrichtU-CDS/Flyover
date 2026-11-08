#!/bin/sh
# Start, stop or check Flyover, on Linux, macOS and Windows (in WSL2 or Git
# Bash, where Docker Desktop's `docker` is at hand).
#
#   ./flyover.sh                 start Flyover (the same as `start`)
#   ./flyover.sh start           create every container, then start Flyover;
#       --build                  rebuild the images built here first
#       --pull always            fetch the newest published images first
#   ./flyover.sh stop            stop Flyover; the RDF store keeps running
#       --all                    stop the RDF store too: it then stays off
#                                until you click Start in Flyover
#   ./flyover.sh status          which containers run, and where the store stands
#
# Flyover starts the RDF store itself once you choose to work on it, in the
# pop-up that asks for the project as you enter the RDF workflow, but it can
# only start a store container that exists.
# `start` therefore creates every container (without starting the store)
# before it starts Flyover, so a `docker compose down` never leaves Flyover
# without a store to start. It never recreates or stops a container that
# exists, so a store other tools use keeps running, and a store stopped on
# purpose (by `stop --all`, say) stays off until you click Start in Flyover's
# status bar.
set -eu

FLYOVER_URL="${FLYOVER_URL:-http://localhost:5000}"

cd "$(dirname "$0")"

say() {
  printf '%s\n' "$*"
}

fail() {
  printf 'flyover.sh: %s\n' "$*" >&2
  exit 1
}

usage() {
  cat <<'EOF'
Usage: ./flyover.sh [start [--build] [--pull always|missing|never] | stop [--all] | status]

  start    create every container, then start Flyover (the default)
  stop     stop Flyover; with --all, the RDF store too
  status   which containers run, and where Flyover's store stands

On Windows, run it in WSL2 or Git Bash.
EOF
}

need_docker() {
  command -v docker >/dev/null 2>&1 ||
    fail "Docker is not installed: see https://docs.docker.com/get-docker/"
  docker info >/dev/null 2>&1 ||
    fail "Docker is not running: start Docker Desktop (or the Docker service) first."
  version=$(docker compose version --short 2>/dev/null) ||
    fail "Docker Compose v2 is missing: update Docker."
  version=${version#v}
  major=${version%%.*}
  rest=${version#*.}
  minor=${rest%%.*}
  case "$major$minor" in
    "" | *[!0-9]*) return 0 ;; # a version this cannot read: try anyway
  esac
  if [ "$major" -lt 2 ] || { [ "$major" -eq 2 ] && [ "$minor" -lt 20 ]; }; then
    fail "Docker Compose $version is too old: Flyover needs 2.20 or newer."
  fi
}

have_curl() {
  command -v curl >/dev/null 2>&1
}

# Where Flyover says its store stands: state, then reason, one per line.
store_status() {
  answer=$(curl -fsS "$FLYOVER_URL/api/v1/project/store-status" 2>/dev/null) || return 1
  # A newline after each: seds differ on input that ends without one.
  printf '%s\n' "$answer" | sed -n 's/.*"state":"\([^"]*\)".*/\1/p'
  printf '%s\n' "$answer" | sed -n 's/.*"reason":"\([^"]*\)".*/\1/p'
}

describe_store() {
  have_curl || return 0
  lines=$(store_status) || {
    say "Flyover does not answer at $FLYOVER_URL."
    return 0
  }
  state=$(printf '%s\n' "$lines" | sed -n 1p)
  reason=$(printf '%s\n' "$lines" | sed -n 2p)
  case "$state" in
    waiting) say "Enter the RDF workflow there and choose a project, or continue without one: Flyover then starts the RDF store." ;;
    starting) say "Flyover is starting the RDF store for what you chose; it takes a minute." ;;
    reachable) say "The RDF store answers." ;;
    stopped) say "The RDF store was stopped: start it from Flyover's status bar, or with: docker start rdf-store" ;;
    *) say "The RDF store: ${reason:-$state}" ;;
  esac
}

wait_for_flyover() {
  have_curl || return 0
  printf 'Waiting for Flyover'
  tries=0
  while ! curl -fsS -o /dev/null "$FLYOVER_URL/" 2>/dev/null; do
    tries=$((tries + 1))
    if [ "$tries" -ge 60 ]; then
      printf '\n'
      fail "Flyover did not answer within two minutes; see: docker compose logs flyover"
    fi
    printf '.'
    sleep 2
  done
  printf '\n'
}

start() {
  build=""
  pull=""
  while [ $# -gt 0 ]; do
    case "$1" in
      --build) build="--build" ;;
      --pull)
        [ $# -ge 2 ] || fail "--pull needs a policy: always, missing or never"
        pull="--pull=$2"
        shift
        ;;
      --pull=*) pull="$1" ;;
      *) fail "start does not know $1 (see ./flyover.sh --help)" ;;
    esac
    shift
  done
  need_docker
  # Every container, the stores' too, none of them started; the ones that
  # exist stay exactly as they are.
  # shellcheck disable=SC2086 # empty options must vanish
  docker compose create --no-recreate $build $pull
  # Flyover and its store launcher.
  # shellcheck disable=SC2086
  docker compose up -d $build $pull flyover
  wait_for_flyover
  say "Flyover runs at $FLYOVER_URL"
  describe_store
}

stop() {
  all=""
  while [ $# -gt 0 ]; do
    case "$1" in
      --all) all="yes" ;;
      *) fail "stop does not know $1 (see ./flyover.sh --help)" ;;
    esac
    shift
  done
  need_docker
  if [ -n "$all" ]; then
    # `stop` keeps the containers, so Flyover can start the store again.
    docker compose stop
    say "Flyover and the RDF store have stopped. The store stays off until you click"
    say "Start in Flyover's status bar (or run: docker start rdf-store)."
  else
    docker compose stop flyover store-launcher
    say "Flyover has stopped; the RDF store keeps running for anything else that uses it."
    say "To stop it too: ./flyover.sh stop --all"
  fi
}

status() {
  need_docker
  docker compose ps -a
  say ""
  describe_store
}

command="${1:-start}"
if [ $# -gt 0 ]; then
  shift
fi
case "$command" in
  start) start "$@" ;;
  stop) stop "$@" ;;
  status) status ;;
  -h | --help | help) usage ;;
  *)
    usage >&2
    exit 2
    ;;
esac
