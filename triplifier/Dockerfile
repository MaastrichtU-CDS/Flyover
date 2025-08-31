FROM python:3.13-slim

WORKDIR /app

# Copy only the requirements file first to leverage Docker cache
COPY requirements.txt /app/

# Install dependencies in a single layer
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
    wget \
    curl \
    default-jre-headless \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir -r requirements.txt

# Set Java environment variables
ENV JAVA_HOME /usr/lib/jvm/default-java
ENV PATH $PATH:$JAVA_HOME/bin

# Set default JAVA_OPTS (can be overridden via environment)
ENV JAVA_OPTS="-Xms2g -Xmx8g"

# Download PostgreSQL JDBC driver
ENV POST_URL https://jdbc.postgresql.org/download/postgresql-42.2.5.jar
RUN wget ${POST_URL} && \
    rm -rf /root/.cache /tmp/*

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
    "data_descriptor.app:app"]