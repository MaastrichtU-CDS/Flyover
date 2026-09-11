FROM python:3.12-slim

LABEL org.opencontainers.image.source="https://github.com/MaastrichtU-CDS/Flyover"
LABEL org.opencontainers.image.description="RDF4J/GraphDB-compatible REST adapter for QLever (Flyover project)"

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY adapter.py /app/adapter.py

EXPOSE 7200

# A generous timeout: uploads can be large and are converted to SPARQL UPDATE
# before being forwarded to QLever.
CMD ["gunicorn", "--bind", "0.0.0.0:7200", "--workers", "2", "--threads", "4", "--timeout", "3600", "adapter:app"]
