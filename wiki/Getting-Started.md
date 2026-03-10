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
| RDF repository | [http://localhost:7200](http://localhost:7200) | Browse the semantic store (GraphDB) |

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

Flyover guides you through four distinct steps:

1. **Ingest** — Upload your data (CSV files or connect to a PostgreSQL database). Flyover converts your data into a structured, semantic representation and stores it in the repository.
2. **Describe** — Provide metadata for your variables: specify data types, define properties, and add semantic context to each column in your dataset. Optionally supply a [JSON-LD semantic map](JSON-LD-Mapping-Format.md) to pre-populate descriptions.
3. **Annotate** — Link your variables to standardised ontologies and terminologies using the built-in annotation interface. A filled-in [JSON-LD semantic map](JSON-LD-Mapping-Format.md) for your data model is needed to drive the annotation process. Review and verify annotations to ensure semantic correctness.
4. **Share** — Download filled-in semantic maps, generate anonymous mock data that preserves the structure of your dataset, and share your project with a wider audience.

## Next Steps

- Learn about the [JSON-LD Mapping Format](JSON-LD-Mapping-Format.md) used for semantic mappings
- Explore the [Example Data](Example-Data.md) bundled with the repository
- If you have existing mappings in the old JSON format, see [Migrating from Legacy Format](Migrating-from-Legacy-Format.md)
