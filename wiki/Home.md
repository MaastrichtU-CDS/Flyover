# Flyover Wiki

Welcome to the Flyover Wiki! This wiki provides detailed documentation on how to use the Flyover Data FAIR-ification tool.

## Pages

- **[Getting Started](Getting-Started.md)** — How to set up and run Flyover
- **[JSON-LD Mapping Format](JSON-LD-Mapping-Format.md)** — How to define semantic mappings using JSON-LD
- **[Annotation Helper](Annotation-Helper.md)** — How to use the annotation helper script
- **[Example Data](Example-Data.md)** — Overview of the bundled example datasets
- **[Migrating from Legacy Format](Migrating-from-Legacy-Format.md)** — How to convert old JSON maps to JSON-LD

## Overview

Flyover is a dockerized Data FAIR-ification tool that converts clinical datasets into Resource Descriptor Format (RDF). It provides:

1. **Data Descriptor Module** — A web interface for uploading and describing data (CSV or PostgreSQL), which is then triplified and stored in an RDF repository.
2. **Annotation Helper** — A script-based tool for creating variable-level semantic annotations using JSON-LD mapping files.
3. **Share Module** — Tools for publishing anonymous metadata and generating mock data.

For a high-level introduction, see the [README](https://github.com/MaastrichtU-CDS/Flyover#readme). For the published paper, see [doi:10.1093/bjrai/ubae005](https://doi.org/10.1093/bjrai/ubae005).
