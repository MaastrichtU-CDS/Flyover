# Flyover
<p align="center">
<a href="https://doi.org/10.5281/zenodo.17419799"><img alt="DOI: 10.5281/zenodo.17419799" src="https://zenodo.org/badge/DOI/10.5281/zenodo.17419799.svg"></a>
<a href="https://opensource.org/licenses/Apache-2.0"><img alt="Licence: Apache 2.0" src="https://img.shields.io/badge/Licence-Apache%202.0-blue.svg"></a>
<br>
<a href="https://www.ontotext.com/products/graphdb/"><img alt="Ontotext GraphDB 10.8.5" src="https://img.shields.io/badge/Ontotext%20GraphDB-v10.8.5-002b4f.svg"></a>
<a href="https://github.com/MaastrichtU-CDS/Triplifier"><img alt="Triplifier version 2.0.0" src="https://img.shields.io/badge/Triplifier%20Version-v2.0.0-purple"></a>
<br>
<a href="https://www.python.org/downloads/"><img alt="Python 3.13+" src="https://img.shields.io/badge/python-3.13+-blue.svg"></a>
<a href="https://github.com/psf/black"><img alt="Code style: black" src="https://img.shields.io/badge/code%20style-black-000000.svg"></a>
<a href="https://flake8.pycqa.org/"><img alt="Linting: flake8" src="https://img.shields.io/badge/linting-flake8-informational"></a>
<a href="http://mypy-lang.org/"><img alt="Type checking: mypy" src="https://img.shields.io/badge/type%20checking-mypy-informational"></a>
<a href="https://github.com/PyCQA/bandit"><img alt="Security: bandit" src="https://img.shields.io/badge/security-bandit-informational"></a>
<a href="https://github.com/pyupio/safety"><img alt="Security: safety" src="https://img.shields.io/badge/security-safety-informational"></a>
</p>

## Introduction

Flyover is a dockerized Data FAIR-ification tool that converts clinical datasets (CSV or PostgreSQL) into Resource Descriptor Format (RDF). It provides a web interface for uploading, describing, annotating, and sharing your data using semantic web standards.

For a detailed explanation, please refer to the published [paper](https://doi.org/10.1093/bjrai/ubae005).

> 📖 **For detailed documentation, see the [Wiki](https://github.com/MaastrichtU-CDS/Flyover/wiki).**

https://github.com/user-attachments/assets/c6678684-4721-4963-83d1-a91582ce2fe1

## Quick Start

Clone the repository and start the services:

```bash
docker-compose up -d
```

| Service | URL | Description |
|---|---|---|
| Web interface | [http://localhost:5000](http://localhost:5000) | Upload and describe your data |
| RDF repository | [http://localhost:7200](http://localhost:7200) | Browse the RDF store (GraphDB) |

> **Note:** On Windows, please use WSL2 with Docker. On macOS/Linux, Docker can be used directly.

See the wiki's [Getting Started](https://github.com/MaastrichtU-CDS/Flyover/wiki/Getting-Started) page for more details on configuration and environment variables.

## Components

### Data Descriptor Module

A web interface that guides you through a multi-step process:

1. **Ingest** — Upload CSV files or connect to a PostgreSQL database. The Triplifier converts your data into RDF triples and stores them in the RDF repository.
2. **Describe** — Describe your variables, their data types, and semantics.
3. **Annotate** — Apply semantic annotations using [JSON-LD mapping files](https://github.com/MaastrichtU-CDS/Flyover/wiki/JSON-LD-Mapping-Format).
4. **Share** — Publish anonymous metadata and generate mock data for sharing.

### Annotation Helper Script

A Python script that automates semantic annotation by reading a JSON-LD mapping file and generating SPARQL queries against the RDF repository.

```bash
python triplifier/data_descriptor/annotation_helper/main.py
```

See the wiki's [Annotation Helper](https://github.com/MaastrichtU-CDS/Flyover/wiki/Annotation-Helper) page for usage details, and the [JSON-LD Mapping Format](https://github.com/MaastrichtU-CDS/Flyover/wiki/JSON-LD-Mapping-Format) page for how to write mapping files.

### Example Data

The `example_data/` folder contains synthetic datasets and JSON-LD mapping files for two fictitious centres, demonstrating all supported features. See the wiki's [Example Data](https://github.com/MaastrichtU-CDS/Flyover/wiki/Example-Data) page for details.

> **Migrating from the old JSON format?** See [Migrating from Legacy Format](https://github.com/MaastrichtU-CDS/Flyover/wiki/Migrating-from-Legacy-Format).

## Wiki

The [Wiki](https://github.com/MaastrichtU-CDS/Flyover/wiki) contains detailed documentation on:

- [Getting Started](https://github.com/MaastrichtU-CDS/Flyover/wiki/Getting-Started) — Setup, configuration, and workflow
- [JSON-LD Mapping Format](https://github.com/MaastrichtU-CDS/Flyover/wiki/JSON-LD-Mapping-Format) — Variable definitions, schema reconstruction, and value mapping
- [Annotation Helper](https://github.com/MaastrichtU-CDS/Flyover/wiki/Annotation-Helper) — Running the annotation script
- [Example Data](https://github.com/MaastrichtU-CDS/Flyover/wiki/Example-Data) — Bundled example datasets
- [Migrating from Legacy Format](https://github.com/MaastrichtU-CDS/Flyover/wiki/Migrating-from-Legacy-Format) — Converting old JSON mappings to JSON-LD

## Developers

- Varsha Gouthamchand
- Joshi Hogenboom
- Johan van Soest
- Leonard Wee
