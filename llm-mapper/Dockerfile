FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy your code
COPY . /app

# Default command (compose overrides it, but nice to have)
CMD ["python", "mapping_recommendation.py"]

