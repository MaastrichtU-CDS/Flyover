# Flyover task recipes. Run `just` (or `just --list`) to see everything.
#
# Install just: `cargo install just`, `brew install just`, or
# `apt install just` (Ubuntu 22.10+). See https://just.systems/ for others.

# Show the menu by default.
default:
    @just --list

# --------------------------------------------------------------------------
# Dev — two-process workflow: backend in docker, frontend on the host
# --------------------------------------------------------------------------

# Backend hot-reload stack (Flask --reload + bind-mount). Foreground; Ctrl+C stops it. Pair with `just dev-frontend`.
dev:
    docker compose -f docker-compose.yml -f docker-compose.dev.yml up

# Vite dev server with HMR at http://localhost:5173/. Proxies API calls to Flask on :5000.
dev-frontend:
    cd frontend && npm run dev

# Backend hot-reload stack + Ollama for LLM mapping suggestions (CPU inference).
dev-llm:
    docker compose -f docker-compose.yml -f docker-compose.dev.yml -f docker-compose.llm.yml up

# Prod-like stack + Ollama with NVIDIA GPU acceleration (requires the NVIDIA container runtime).
up-llm-gpu:
    docker compose -f docker-compose.yml -f docker-compose.llm.yml -f docker-compose.llm-gpu.yml up

# --------------------------------------------------------------------------
# Tests
# --------------------------------------------------------------------------

# Run every fast test layer (backend pytest + frontend vitest). No stack required.
test: test-backend test-frontend

# Backend pytest — unit + integration.
test-backend:
    cd backend/flyover && uv run --with-requirements ../requirements.txt --with pytest --with pytest-mock python -m pytest tests/ -q

# Backend unit tests only (fast, mocked GraphDB).
test-backend-unit:
    cd backend/flyover && uv run --with-requirements ../requirements.txt --with pytest --with pytest-mock python -m pytest tests/unit/ -q

# Backend integration tests only (loaders + validator against example_data).
test-backend-integration:
    cd backend/flyover && uv run --with-requirements ../requirements.txt --with pytest --with pytest-mock python -m pytest tests/integration/ -q

# Frontend Vitest unit suite.
test-frontend:
    cd frontend && npm run test:unit

# Playwright smoke spec against the tmpfs test stack (~30s).
test-e2e:
    ./scripts/start-test-stack.sh
    cd frontend && npx playwright test tests/e2e/smoke.spec.js --reporter=line
    docker compose -f docker-compose.yml -f docker-compose.test.yml down -v

# Full Playwright suite against the tmpfs test stack (a few minutes).
test-e2e-full:
    ./scripts/start-test-stack.sh
    cd frontend && npm run test:e2e
    docker compose -f docker-compose.yml -f docker-compose.test.yml down -v

# --------------------------------------------------------------------------
# Stack / housekeeping
# --------------------------------------------------------------------------

# Tear down any running Flyover compose stack (dev, prod-like, or test). Safe to run idempotently.
down:
    -docker compose -f docker-compose.yml -f docker-compose.dev.yml down
    -docker compose -f docker-compose.yml -f docker-compose.test.yml down -v
    -docker compose -f docker-compose.yml -f docker-compose.llm.yml down
    -docker compose down

# --------------------------------------------------------------------------
# Code quality
# --------------------------------------------------------------------------

# Format Python (ruff) and JS/Vue (eslint --fix).
format:
    cd backend && uv run --with ruff ruff format flyover/
    cd frontend && npm run lint -- --fix

# Lint Python (ruff) + JS/Vue (eslint) + types (vue-tsc).
lint:
    cd backend && uv run --with ruff ruff check flyover/
    cd frontend && npm run lint
    cd frontend && npm run type-check

# --------------------------------------------------------------------------
# Build
# --------------------------------------------------------------------------

# Build the production Docker image (same image release.yaml publishes).
build:
    docker compose -f docker-compose.yml build
