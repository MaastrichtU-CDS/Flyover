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

# Download PostgreSQL JDBC driver
ENV POST_URL https://jdbc.postgresql.org/download/postgresql-42.2.5.jar
RUN wget ${POST_URL} && \
    rm -rf /root/.cache /tmp/*

# Now copy the application code
COPY data_descriptor/ /app/data_descriptor/

CMD ["python3", "/app/data_descriptor/data_descriptor_main.py"]