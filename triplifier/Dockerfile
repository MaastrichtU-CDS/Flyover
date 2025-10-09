FROM python:3.13-alpine
WORKDIR /app

# Copy only the requirements file first to leverage Docker cache
COPY requirements.txt /app/

# Install build dependencies (removed after use)
RUN apk add --no-cache --virtual .build-deps \
    gcc \
    python3-dev \
    musl-dev \
    libpq \
    postgresql-dev \
    git \
    ca-certificates \
    && apk add --no-cache \
    curl \
    libpq

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Remove build dependencies
RUN apk del .build-deps

# Copy the application code
COPY data_descriptor/ /app/data_descriptor/

# Use Gunicorn as the WSGI server
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