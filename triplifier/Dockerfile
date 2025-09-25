FROM python:3.13-slim

WORKDIR /app

# Copy only the requirements file first to leverage Docker cache
COPY requirements.txt /app/

# Install dependencies in a single layer (removed Java dependencies)
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
    git \
    libpq-dev \
    ca-certificates \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org --no-cache-dir -r requirements.txt

# Now copy the application code
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