# Flyover Wiki

Welcome to the Flyover Wiki! This wiki provides detailed documentation on how to use the Flyover Data FAIR-ification tool.

## Pages

- **[Getting Started](Getting-Started.md)** — How to set up and run Flyover
- **[JSON-LD Mapping Format](JSON-LD-Mapping-Format.md)** — How to define semantic mappings using JSON-LD
- **[Example Data](Example-Data.md)** — Overview of the bundled example datasets
- **[Migrating from Legacy Format](Migrating-from-Legacy-Format.md)** — How to convert old JSON maps to JSON-LD

## Overview

Flyover is a dockerised Data FAIR-ification tool that transforms structured datasets into semantically enriched, interoperable formats. It works with CSV files and PostgreSQL databases, and uses linked data standards such as RDF and JSON-LD for its semantic representations.

Flyover guides you through four steps:

1. **Ingest** — Upload your data, and Flyover converts your data into a structured, semantic representation.
2. **Describe** — Provide metadata for your variables: data types, properties, and semantic context. Optionally you can supply a JSON-LD semantic map.
3. **Annotate** — Link your variables to standardised ontologies and verify the annotations. For this you need to provide a filled-in JSON-LD semantic map for your data model.
4. **Share** — Download filled-in semantic maps, generate anonymous mock data, and share your project with a wider audience.

For a high-level introduction, see the [README](https://github.com/MaastrichtU-CDS/Flyover#readme).
