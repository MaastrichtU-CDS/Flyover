# --- Stage 1: build the Vue frontend ---
FROM node:20-alpine AS frontend-build
WORKDIR /frontend
COPY frontend/package*.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

# --- Stage 2: build the independent React/TypeScript v2 frontend ---
FROM node:20-alpine AS frontend-v2-build
WORKDIR /frontend-v2
COPY frontend-v2/package*.json ./
RUN npm ci
COPY frontend-v2/ ./
RUN npm run build

# --- Stage 3: Python backend ---
FROM python:3.13-slim
WORKDIR /app

COPY backend/requirements.txt .

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    libpq-dev \
    ca-certificates \
    curl \
    && pip install --no-cache-dir -r requirements.txt \
    && apt-get purge -y --auto-remove gcc python3-dev \
    && rm -rf /var/lib/apt/lists/*

COPY backend/data_descriptor/ /app/data_descriptor/
COPY backend/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Built SPA gets served by Flask under /app/
COPY --from=frontend-build /frontend/dist /app/data_descriptor/spa
COPY --from=frontend-v2-build /frontend-v2/dist /app/data_descriptor/spa-v2

WORKDIR /app
ENTRYPOINT ["/app/entrypoint.sh"]
