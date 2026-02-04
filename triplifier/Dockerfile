FROM python:3.13-slim
WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY requirements.txt .

# Install build dependencies, Python packages, and clean up
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    libpq-dev \
    ca-certificates \
    curl \
    && pip install --no-cache-dir -r requirements.txt \
    && apt-get purge -y --auto-remove gcc python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy the entire application code
COPY data_descriptor/ /app/data_descriptor/

# Copy and prepare entrypoint script
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Ensure the working directory is set correctly
WORKDIR /app

# Run Gunicorn via entrypoint script (supports GUNICORN_TIMEOUT env var)
ENTRYPOINT ["/app/entrypoint.sh"]
