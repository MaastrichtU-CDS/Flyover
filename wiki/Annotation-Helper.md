# Annotation Helper

The Annotation Helper is a Python script that reads a JSON-LD semantic mapping file and generates SPARQL annotation queries, which are then sent to the RDF repository. This automates the process of creating variable-level semantic annotations for your data.

## Prerequisites

- Flyover services running (`docker-compose up -d`)
- Data already ingested and triplified via the web interface
- A JSON-LD mapping file describing your variables (see [JSON-LD Mapping Format](JSON-LD-Mapping-Format.md))
- Python 3.13+

## Preparing Your Mapping File

Create a `.jsonld` mapping file that defines:

1. **The endpoint** — URL of your RDF repository's SPARQL endpoint
2. **The schema** — Variable definitions with predicates, classes, schema reconstructions, and value mappings
3. **The database mappings** — How your local columns and values map to the schema

See the [JSON-LD Mapping Format](JSON-LD-Mapping-Format.md) page for the full specification and examples. A template is available at `example_data/mapping_template.jsonld`.

## Running the Script

From the repository folder:

```bash
python triplifier/data_descriptor/annotation_helper/main.py
```

The script will:

1. Read the JSON-LD mapping file
2. Extract variable definitions from the schema
3. Generate SPARQL queries based on the SPARQL templates
4. Send the queries to the configured RDF endpoint

## Evaluating the Annotation Process

By default, the script logs the annotation process and saves generated SPARQL queries:

- **Log file**: `annotation_log.txt` — located in the same folder as your JSON-LD mapping file
- **Generated queries**: `generated_queries/` folder — contains `.rq` files organised per variable, also in the same folder as your mapping file

If the log indicates that annotations were unsuccessful for certain variables, inspect the generated `.rq` files for those variables to diagnose the issue.

## SPARQL Templates

The annotation helper uses SPARQL template files in which Python fills in variable-specific information from the mapping file. The templates are located at:

```
triplifier/data_descriptor/annotation_helper/src/sparql_templates/
```

- `template_mapping.rq` — Template for mapping queries
- `template_annotation.rq` — Template for annotation queries
