# Flyover Software Architecture

## Modular Transformation Design

This document describes the software architecture of Flyover with a focus on modular transformation capabilities.

## Table of Contents

- [Overview](#overview)
- [High-Level Architecture](#high-level-architecture)
- [Core Components](#core-components)
- [Transformation Pipeline](#transformation-pipeline)
- [Module Interfaces](#module-interfaces)
- [Data Flow](#data-flow)
- [Extension Points](#extension-points)

---

## Overview

Flyover is a dockerized Data FAIR-ification tool that converts clinical datasets into Resource Descriptor Format (RDF). The architecture follows a modular design that separates concerns into distinct, reusable components.

### Design Principles

1. **Modularity**: Each transformation step is encapsulated in its own module
2. **Extensibility**: New data sources and transformation types can be added without modifying core code
3. **Separation of Concerns**: Clear boundaries between data ingestion, transformation, and storage
4. **Template-Based**: SPARQL templates enable flexible schema reconstruction

---

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              FLYOVER SYSTEM                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐   │
│  │   INGEST    │───▶│   TRIPLIFY  │───▶│  DESCRIBE   │───▶│  ANNOTATE   │   │
│  │   Module    │    │   Module    │    │   Module    │    │   Module    │   │
│  └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘   │
│         │                  │                  │                  │          │
│         ▼                  ▼                  ▼                  ▼          │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                        GRAPHDB (RDF Store)                          │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐               │   │
│  │  │ data.local   │  │ontology.local│  │annotation    │               │   │
│  │  │    Graph     │  │    Graph     │  │local Graph   │               │   │
│  │  └──────────────┘  └──────────────┘  └──────────────┘               │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Core Components

### 1. Data Ingest Module (`utils/data_ingest.py`)

**Responsibility**: Upload and store data in the RDF repository.

```
┌─────────────────────────────────────────┐
│           Data Ingest Module            │
├─────────────────────────────────────────┤
│ • upload_file_to_graphdb()              │
│ • upload_ontology_then_data()           │
│ • Background upload support             │
│ • Fallback upload strategies            │
└─────────────────────────────────────────┘
```

**Key Features**:
- Sequential upload (ontology → data) for data integrity
- Automatic fallback between `--data-binary` and streaming methods
- Background upload support using gevent greenlets
- Configurable timeouts per file type

### 2. Data Preprocessing Module (`utils/data_preprocessing.py`)

**Responsibility**: Clean and prepare data for triplification.

```
┌─────────────────────────────────────────┐
│        Data Preprocessing Module        │
├─────────────────────────────────────────┤
│ • clean_column_names()                  │
│ • preprocess_dataframe()                │
│ • _sanitise_column_name()               │
│ • _handle_duplicate_columns()           │
└─────────────────────────────────────────┘
```

**Key Features**:
- Column name sanitization for HTML/JavaScript safety
- Duplicate column handling
- Original column mapping preservation
- Character replacement for special characters

### 3. Triplifier Integration Module (`utils/python_triplifier_integration.py`)

**Responsibility**: Convert tabular data to RDF triples.

```
┌─────────────────────────────────────────┐
│    Python Triplifier Integration        │
├─────────────────────────────────────────┤
│ • PythonTriplifierIntegration class     │
│   ├── run_triplifier_csv()              │
│   └── run_triplifier_sql()              │
│ • run_triplifier() factory function     │
└─────────────────────────────────────────┘
```

**Key Features**:
- Support for CSV and PostgreSQL data sources
- Automatic SQLite intermediate database for CSV processing
- YAML configuration generation
- Ontology and data output generation

### 4. Annotation Helper Module (`annotation_helper/`)

**Responsibility**: Add semantic annotations to the RDF graph.

```
┌─────────────────────────────────────────┐
│         Annotation Helper Module        │
├─────────────────────────────────────────┤
│ src/                                    │
│ ├── miscellaneous.py                    │
│ │   ├── add_annotation()                │
│ │   ├── add_mapping()                   │
│ │   ├── _construct_extra_class()        │
│ │   ├── _construct_extra_node()         │
│ │   └── _remove_component()             │
│ └── sparql_templates/                   │
│     ├── template_annotation.rq          │
│     ├── template_mapping.rq             │
│     └── schema_reconstruction/          │
│         ├── template_for_extra_class.rq │
│         ├── template_for_extra_node.rq  │
│         └── template_to_remove_...rq    │
└─────────────────────────────────────────┘
```

**Key Features**:
- Template-based SPARQL query generation
- Schema reconstruction (extra classes, nodes)
- Value mapping between local and standard terminologies
- Quality control checks for annotations

---

## Transformation Pipeline

The transformation pipeline consists of four sequential stages:

### Stage 1: Ingest

```
┌──────────────────────────────────────────────────────────────┐
│                     INGEST STAGE                             │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  Input Sources:                                              │
│  ┌─────────┐  ┌─────────────┐  ┌────────────────────────┐   │
│  │  CSV    │  │  PostgreSQL │  │  Semantic Map (JSON)   │   │
│  │  Files  │  │   Database  │  │  (optional)            │   │
│  └────┬────┘  └──────┬──────┘  └───────────┬────────────┘   │
│       │              │                     │                 │
│       ▼              ▼                     ▼                 │
│  ┌─────────────────────────────────────────────────────┐    │
│  │              Data Preprocessing                      │    │
│  │  • Column name cleaning                              │    │
│  │  • Duplicate handling                                │    │
│  │  • Character sanitization                            │    │
│  └─────────────────────────────────────────────────────┘    │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

### Stage 2: Triplify

```
┌──────────────────────────────────────────────────────────────┐
│                    TRIPLIFY STAGE                            │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌─────────────────────────────────────────────────────┐    │
│  │              Python Triplifier                       │    │
│  │                                                      │    │
│  │  CSV Path:                                           │    │
│  │  DataFrame → SQLite → Triplifier → RDF              │    │
│  │                                                      │    │
│  │  PostgreSQL Path:                                    │    │
│  │  PostgreSQL → Triplifier → RDF                      │    │
│  └─────────────────────────────────────────────────────┘    │
│                           │                                  │
│                           ▼                                  │
│  ┌─────────────────────────────────────────────────────┐    │
│  │                 Output Files                         │    │
│  │  • ontology.owl (OWL schema)                         │    │
│  │  • output.ttl (RDF data in Turtle format)           │    │
│  └─────────────────────────────────────────────────────┘    │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

### Stage 3: Describe

```
┌──────────────────────────────────────────────────────────────┐
│                    DESCRIBE STAGE                            │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌─────────────────────────────────────────────────────┐    │
│  │              Variable Description UI                 │    │
│  │                                                      │    │
│  │  For each column:                                    │    │
│  │  • Specify data type (Categorical/Continuous/ID)     │    │
│  │  • Map to global variable name                       │    │
│  │  • Add comments and descriptions                     │    │
│  └─────────────────────────────────────────────────────┘    │
│                           │                                  │
│                           ▼                                  │
│  ┌─────────────────────────────────────────────────────┐    │
│  │           Category/Unit Details                      │    │
│  │                                                      │    │
│  │  Categorical: Map local values to standard terms     │    │
│  │  Continuous: Specify units of measurement            │    │
│  └─────────────────────────────────────────────────────┘    │
│                           │                                  │
│                           ▼                                  │
│  ┌─────────────────────────────────────────────────────┐    │
│  │              Local Semantic Map                      │    │
│  │  • Database-specific mappings                        │    │
│  │  • Local definitions added to global map             │    │
│  └─────────────────────────────────────────────────────┘    │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

### Stage 4: Annotate

```
┌──────────────────────────────────────────────────────────────┐
│                    ANNOTATE STAGE                            │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌─────────────────────────────────────────────────────┐    │
│  │              Annotation Helper                       │    │
│  │                                                      │    │
│  │  1. Schema Reconstruction                            │    │
│  │     • Add extra classes (before/after placement)     │    │
│  │     • Add extra nodes (units, etc.)                  │    │
│  │                                                      │    │
│  │  2. Variable Annotation                              │    │
│  │     • Predicate assignment                           │    │
│  │     • Class equivalence (owl:equivalentClass)        │    │
│  │                                                      │    │
│  │  3. Value Mapping                                    │    │
│  │     • Map local terms to standard ontologies         │    │
│  │     • Create rdfs:subClassOf relationships           │    │
│  └─────────────────────────────────────────────────────┘    │
│                           │                                  │
│                           ▼                                  │
│  ┌─────────────────────────────────────────────────────┐    │
│  │              Annotation Graph                        │    │
│  │  Named Graph: http://annotation.local/               │    │
│  └─────────────────────────────────────────────────────┘    │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

---

## Module Interfaces

### Data Source Interface

```python
# Abstract interface for data sources (conceptual)
class DataSourceInterface:
    """Interface for pluggable data sources."""
    
    def validate_connection(self) -> bool:
        """Validate that the data source is accessible."""
        pass
    
    def get_schema(self) -> dict:
        """Return schema information (tables, columns, types)."""
        pass
    
    def extract_data(self) -> pd.DataFrame:
        """Extract data as a pandas DataFrame."""
        pass
```

**Current Implementations**:
- CSV files via `pd.read_csv()`
- PostgreSQL via `psycopg2` connection

### Transformation Interface

```python
# Abstract interface for transformations (conceptual)
class TransformationInterface:
    """Interface for pluggable transformation steps."""
    
    def transform(self, input_data: Any) -> Any:
        """Apply transformation to input data."""
        pass
    
    def validate(self, output_data: Any) -> bool:
        """Validate transformation output."""
        pass
```

**Current Implementations**:
- `preprocess_dataframe()` - Data cleaning
- `run_triplifier()` - RDF conversion
- `add_annotation()` - Semantic annotation

### Template Interface

```
SPARQL Template Structure:
┌─────────────────────────────────────────────────────────────┐
│                    SPARQL Template                          │
├─────────────────────────────────────────────────────────────┤
│ # Prefixes                                                  │
│ PREFIX db: <http://data.local/rdf/ontology/>               │
│ PREFIX dbo: <http://um-cds/ontologies/databaseontology/>   │
│ PREFIX PLACEHOLDER: <>                                      │
│                                                             │
│ # INSERT clause                                             │
│ INSERT {                                                    │
│     GRAPH <http://annotation.local/> {                      │
│         # Placeholder-based triples                         │
│     }                                                       │
│ }                                                           │
│                                                             │
│ # WHERE clause                                              │
│ WHERE {                                                     │
│     # Pattern matching                                      │
│ }                                                           │
└─────────────────────────────────────────────────────────────┘
```

**Template Placeholders**:
| Placeholder | Description |
|-------------|-------------|
| `databasename` | Name of the database/table |
| `localvariable` | Local variable name |
| `PLACEHOLDER:variablepredicate` | Predicate URI |
| `PLACEHOLDER:variableclass` | Class URI |
| `reconstructionlabel` | Label for schema reconstruction |
| `reconstructionaestheticlabel` | Human-readable label |
| `PLACEHOLDER:reconstructionclass` | Reconstruction class URI |

---

## Data Flow

```
                                    ┌──────────────────┐
                                    │  Semantic Map    │
                                    │     (JSON)       │
                                    └────────┬─────────┘
                                             │
┌───────────────┐    ┌───────────────┐      │      ┌───────────────┐
│  CSV Files    │───▶│   DataFrame   │──────┴─────▶│  Triplifier   │
└───────────────┘    │ Preprocessing │             │               │
                     └───────────────┘             └───────┬───────┘
                                                           │
┌───────────────┐                                          │
│  PostgreSQL   │──────────────────────────────────────────┤
│   Database    │                                          │
└───────────────┘                                          │
                                                           ▼
                                              ┌────────────────────┐
                                              │   ontology.owl     │
                                              │   output.ttl       │
                                              └─────────┬──────────┘
                                                        │
                                                        ▼
                                              ┌────────────────────┐
                                              │     GraphDB        │
                                              │ ┌────────────────┐ │
                                              │ │ ontology.local │ │
                                              │ │ data.local     │ │
                                              │ └────────────────┘ │
                                              └─────────┬──────────┘
                                                        │
                              ┌──────────────────────────┴──────────────────────────┐
                              │                                                      │
                              ▼                                                      ▼
                    ┌─────────────────┐                                  ┌─────────────────┐
                    │    Describe     │                                  │    Annotate     │
                    │    Variables    │─────────────────────────────────▶│    Helper       │
                    │                 │                                  │                 │
                    │ • Data types    │      Local Semantic Map          │ • SPARQL        │
                    │ • Mappings      │ ─────────────────────────────▶   │   Templates     │
                    │ • Categories    │                                  │ • Schema        │
                    └─────────────────┘                                  │   Reconstruct   │
                                                                         └────────┬────────┘
                                                                                  │
                                                                                  ▼
                                                                        ┌─────────────────┐
                                                                        │ annotation.local│
                                                                        │     Graph       │
                                                                        └─────────────────┘
```

---

## Extension Points

### 1. Adding New Data Sources

To add a new data source (e.g., MySQL, Excel):

1. Create a new module in `utils/` (e.g., `mysql_integration.py`)
2. Implement data extraction returning a pandas DataFrame
3. Add UI elements in `ingest.html` for the new source type
4. Update `upload_file()` in `data_descriptor_main.py` to handle the new type

```python
# Example: Adding MySQL support
class MySQLIntegration:
    def __init__(self, connection_params: dict):
        self.params = connection_params
    
    def extract_tables(self) -> List[str]:
        """List available tables."""
        pass
    
    def extract_data(self, table: str) -> pd.DataFrame:
        """Extract data from a specific table."""
        pass
```

### 2. Adding New Transformation Steps

To add a new transformation step:

1. Create a function in `utils/` or a new module
2. Integrate into the pipeline in `data_descriptor_main.py`
3. Add appropriate error handling and logging

```python
# Example: Adding data validation step
def validate_data_quality(df: pd.DataFrame) -> Tuple[bool, List[str]]:
    """
    Validate data quality before triplification.
    
    Returns:
        (is_valid, list_of_issues)
    """
    issues = []
    # Check for empty columns
    # Check for invalid values
    # Check for data type consistency
    return len(issues) == 0, issues
```

### 3. Adding New SPARQL Templates

To add new schema reconstruction patterns:

1. Create a new `.rq` file in `sparql_templates/schema_reconstruction/`
2. Use standard placeholders for dynamic content
3. Update `miscellaneous.py` to support the new template

```sparql
# Example: template_for_complex_relationship.rq
PREFIX db: <http://data.local/rdf/ontology/>
PREFIX dbo: <http://um-cds/ontologies/databaseontology/>
# ... standard prefixes ...

INSERT {
    GRAPH <http://annotation.local/> {
        # Your custom pattern here
    }
}
WHERE {
    # Your pattern matching here
}
```

### 4. Adding New Ontology Mappings

The semantic map JSON structure supports extensible ontology mappings:

```json
{
  "variable_info": {
    "new_variable": {
      "data_type": "categorical",
      "predicate": "your:predicate",
      "class": "your:class",
      "local_definition": null,
      "schema_reconstruction": [
        {
          "type": "class",
          "placement": "before",
          "predicate": "your:hasSomething",
          "class": "your:SomeClass",
          "class_label": "someClassLabel",
          "aesthetic_label": "Some Class"
        }
      ],
      "value_mapping": {
        "terms": {
          "value1": {
            "local_term": null,
            "target_class": "your:Value1Class"
          }
        }
      }
    }
  }
}
```

---

## Directory Structure

```
flyover/
├── docker-compose.yml              # Service orchestration
├── graphdb/                        # GraphDB configuration
│   └── Dockerfile
├── triplifier/
│   ├── Dockerfile                  # Triplifier service container
│   ├── requirements.txt            # Python dependencies
│   └── data_descriptor/
│       ├── data_descriptor_main.py # Main Flask application
│       ├── templates/              # Jinja2 HTML templates
│       │   ├── index.html          # Landing page
│       │   ├── ingest.html         # Data upload page
│       │   ├── describe_*.html     # Variable description pages
│       │   └── annotation_*.html   # Annotation pages
│       ├── assets/                 # Static files (CSS, JS, images)
│       ├── utils/
│       │   ├── __init__.py
│       │   ├── data_ingest.py      # GraphDB upload utilities
│       │   ├── data_preprocessing.py # DataFrame cleaning
│       │   └── python_triplifier_integration.py # Triplifier wrapper
│       └── annotation_helper/
│           ├── __init__.py
│           ├── main.py             # CLI entry point
│           └── src/
│               ├── __init__.py
│               ├── miscellaneous.py # Core annotation functions
│               └── sparql_templates/
│                   ├── template_annotation.rq
│                   ├── template_mapping.rq
│                   ├── schema_reconstruction/
│                   │   ├── template_for_extra_class.rq
│                   │   ├── template_for_extra_node.rq
│                   │   └── template_to_remove_component.rq
│                   └── quality_control/
│                       ├── template_to_check_class.rq
│                       └── template_to_check_predicate.rq
└── example_data/                   # Example semantic maps and data
    ├── data_semantic_map.json
    ├── centre_a_english/
    └── centre_b_dutch/
```

---

## Technology Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| Web Framework | Flask | REST API and web UI |
| WSGI Server | Gunicorn + gevent | Production server with async support |
| RDF Store | Ontotext GraphDB | Triple store and SPARQL endpoint |
| Data Processing | pandas | DataFrame manipulation |
| Database Connector | psycopg2 | PostgreSQL integration |
| RDF Generation | Python Triplifier | CSV/SQL to RDF conversion |
| Containerization | Docker | Deployment and isolation |

---

## Future Considerations

### Potential Enhancements

1. **Plugin Architecture**: Formalize the extension points into a plugin system
2. **Pipeline Configuration**: Allow YAML-based pipeline definition
3. **Batch Processing**: Support for processing multiple datasets in parallel
4. **Provenance Tracking**: Add PROV-O annotations for transformation lineage
5. **API Mode**: Expose REST API for programmatic access

### Scalability Considerations

1. **Horizontal Scaling**: Multiple triplifier instances behind load balancer
2. **Caching**: Redis/Memcached for intermediate results
3. **Queue-Based Processing**: Celery/RQ for large dataset processing
4. **Streaming**: Support for streaming large files to GraphDB

---

## References

- [Flyover Paper](https://doi.org/10.1093/bjrai/ubae005)
- [Python Triplifier](https://github.com/MaastrichtU-CDS/Triplifier)
- [Ontotext GraphDB](https://www.ontotext.com/products/graphdb/)
- [FAIR Principles](https://www.go-fair.org/fair-principles/)
