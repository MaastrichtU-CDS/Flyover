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

# Ensure the working directory is set correctly
WORKDIR /app

# Run Gunicorn
CMD ["gunicorn", \
    "--workers", "1", \
    "--worker-class", "gevent", \
    "--worker-connections", "1000", \
    "--timeout", "300", \
    "--keep-alive", "5", \
    "--max-requests", "1000", \
    "--max-requests-jitter", "50", \
    "--log-level", "info", \
    "--access-logfile", "-", \
    "--access-logformat", "%(t)s [ACCESS] %(m)s %(U)s %(s)s request by %(h)s", \
    "--error-logfile", "-", \
    "--bind", "0.0.0.0:5000", \
    "--preload", \
    "--env", "GEVENT_SUPPORT=True", \
    "data_descriptor.data_descriptor_main:app"]
