# --- Stage 1: build the Vue frontend ---
FROM node:20-alpine AS frontend-build
WORKDIR /frontend
COPY frontend/package*.json ./
RUN npm install
COPY frontend/ ./
RUN npm run build

# --- Stage 2: Python backend ---
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

WORKDIR /app
ENTRYPOINT ["/app/entrypoint.sh"]
