# Getting Started

## Prerequisites

- **Docker** and **Docker Compose** installed on your machine
- On Windows, use **WSL2** with Docker
- On macOS/Linux, Docker can be used directly

## Running Flyover

Clone the repository (or download it), then start the services from the project folder:

```bash
docker-compose up -d
```

This will start two services:

| Service | URL | Description |
|---|---|---|
| Web interface | [http://localhost:5000](http://localhost:5000) | Upload and describe your data |
| RDF repository | [http://localhost:7200](http://localhost:7200) | Browse the RDF store (GraphDB) |

## Docker Compose Configuration

The default `docker-compose.yml` starts two containers:

- **rdf-store** — An Ontotext GraphDB instance (port 7200)
- **triplifier** — The Flyover Flask application (port 5000)

### Optional Environment Variables

You can override timeout settings in the `triplifier` service:

```yaml
environment:
  - GUNICORN_TIMEOUT=300       # Web server timeout (seconds)
  - RDF_REQUEST_TIMEOUT=3600   # RDF store request timeout (seconds)
```

## Workflow

Flyover guides you through a multi-step process:

1. **Ingest** — Upload your data (CSV files or connect to a PostgreSQL database). The Triplifier converts them into RDF triples and stores them in the RDF repository.
2. **Describe** — Describe your variables, their data types, and semantics through the web interface.
3. **Annotate** — Apply semantic annotations to your data using JSON-LD mapping files, either through the UI or the [Annotation Helper](Annotation-Helper.md) script.
4. **Share** — Publish anonymous metadata and generate mock data for sharing.

## Next Steps

- Learn about the [JSON-LD Mapping Format](JSON-LD-Mapping-Format.md) used for semantic mappings
- Explore the [Example Data](Example-Data.md) bundled with the repository
- If you have existing mappings in the old JSON format, see [Migrating from Legacy Format](Migrating-from-Legacy-Format.md)
