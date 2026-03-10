# JSON-LD Mapping Format

Since Flyover v3.0.0, semantic mappings use the **JSON-LD** format (`.jsonld` files). This format separates the reusable _schema_ (variable definitions, ontology classes, reconstruction rules) from the _local mappings_ (site-specific column names and value translations).

A template is available at [`example_data/mapping_template.jsonld`](https://github.com/MaastrichtU-CDS/Flyover/blob/main/example_data/mapping_template.jsonld).

## File Structure

A JSON-LD mapping file has the following top-level structure:

```jsonld
{
  "$schema": "https://raw.githubusercontent.com/MaastrichtU-CDS/Flyover/main/triplifier/data_descriptor/schemas/mapping_schema.json",
  "@context": { ... },
  "@id": "your-mapping-id",
  "@type": "mapping:DataMapping",
  "name": "Your Mapping Name",
  "description": "",
  "version": "1.0.0",
  "created": "2025-01-01",
  "endpoint": "",
  "schema": { ... },
  "databases": { ... }
}
```

### `@context`

Defines namespace prefixes and JSON-LD keywords used throughout the file. The default context includes standard ontology prefixes (`sio`, `ncit`, `mesh`, `roo`, `xsd`) plus Flyover-specific terms for `schema` and `mapping`.

### `schema`

Contains the reusable semantic schema — the variable definitions that are shared across sites.

```jsonld
"schema": {
  "@id": "schema:clinical-oncology/v1",
  "@type": "schema:SemanticSchema",
  "name": "Clinical Oncology Semantic Schema",
  "version": "1.0.0",
  "prefixes": { ... },
  "variables": { ... }
}
```

### `databases`

Contains the local, site-specific mappings — how your columns map to the schema variables and what local terminology you use.

## Defining Variables

Each variable in the `schema.variables` object describes a semantic concept. The key is the variable name, and the value contains its definition:

```jsonld
"biological_sex": {
  "@id": "schema:variable/biological_sex",
  "@type": "schema:CategoricalVariable",
  "dataType": "categorical",
  "predicate": "sio:SIO_000008",
  "class": "ncit:C28421"
}
```

### Supported Variable Types

| `@type` | `dataType` | Description |
|---|---|---|
| `schema:IdentifierVariable` | `identifier` | Unique record identifier |
| `schema:CategoricalVariable` | `categorical` | Discrete categories (e.g., male/female) |
| `schema:ContinuousVariable` | `continuous` | Numeric measurements (e.g., age, weight) |
| `schema:OrdinalVariable` | `ordinal` | Ordered categories (e.g., severity levels) |
| `schema:DateVariable` | `date` | Date values |
| `schema:TextVariable` | `text` | Free-text values |

### Required Fields

- `@id` — Unique identifier for the variable (typically `schema:variable/<name>`)
- `@type` — One of the supported variable types above
- `dataType` — String matching the variable type
- `predicate` — The RDF predicate linking the individual to the variable (e.g., `sio:SIO_000008`)
- `class` — The ontology class for this variable (e.g., `ncit:C28421`)

## Schema Reconstruction

Schema reconstruction allows you to modify the graph structure around a variable. Two types of reconstruction nodes are supported:

### Class Node (`schema:ClassNode`)

Adds an intermediate class node to group or contextualize variables.

```jsonld
"schemaReconstruction": [
  {
    "@type": "schema:ClassNode",
    "predicate": "sio:SIO_000235",
    "class": "mesh:D000091569",
    "classLabel": "demographicClass",
    "aestheticLabel": "Demographic"
  }
]
```

**Fields:**
- `predicate` — The RDF predicate for connecting to this class
- `class` — The ontology class URI
- `classLabel` — Internal label used as an identifier
- `aestheticLabel` — Human-readable display label
- `placement` (optional) — `"before"` (default) or `"after"` the variable class:
  - **before**: placed between the individual's class and the variable class
  - **after**: placed between the variable class and the referenced ontology class

### Unit Node (`schema:UnitNode`)

Specifies the unit of measurement for continuous variables.

```jsonld
{
  "@type": "schema:UnitNode",
  "predicate": "sio:SIO_000221",
  "class": "ncit:C29848",
  "nodeLabel": "years",
  "aestheticLabel": "Years"
}
```

### Multiple Reconstructions

You can chain multiple reconstruction nodes. They are applied in order from top to bottom:

```jsonld
"schemaReconstruction": [
  {
    "@type": "schema:ClassNode",
    "predicate": "sio:SIO_000235",
    "class": "mesh:D000091569",
    "classLabel": "demographicClass",
    "aestheticLabel": "Demographic"
  },
  {
    "@type": "schema:ClassNode",
    "placement": "after",
    "predicate": "sio:SIO_000253",
    "class": "ncit:C142529",
    "classLabel": "ehrClass",
    "aestheticLabel": "EHR"
  },
  {
    "@type": "schema:UnitNode",
    "predicate": "sio:SIO_000221",
    "class": "ncit:C29848",
    "nodeLabel": "years",
    "aestheticLabel": "Years"
  }
]
```

## Value Mapping

Value mapping lets you map local categorical values to ontology classes:

```jsonld
"valueMapping": {
  "terms": {
    "male": {
      "targetClass": "ncit:C20197"
    },
    "female": {
      "targetClass": "ncit:C16576"
    },
    "missing_or_unspecified": {
      "targetClass": "ncit:C54031"
    }
  }
}
```

Each key in `terms` is the value as it appears in the schema. The `targetClass` is the ontology class URI that value maps to.

## Local Database Mappings

The `databases` section maps your local data structure to the schema. Site-specific column names, local terminology, and database metadata are defined here.

### Database Fields

- `@id` — Unique identifier for the database
- `@type` — Always `mapping:Database`
- `name` (optional) — Human-readable name for the database
- `description` (optional) — Text description of the database
- `locale` (optional) — Locale code for the data (e.g., `en_GB`, `nl_NL`)
- `endpoint` (optional) — Database-specific endpoint override
- `tables` — Object containing one or more table definitions

### Table Fields

- `@id` — Unique identifier for the table
- `@type` — Always `mapping:Table`
- `sourceFile` (optional) — Source file name or path
- `description` (optional) — Text description of the table
- `columns` — Object containing one or more column mappings

### Column Mapping Fields

- `mapsTo` — Reference to the schema variable this column maps to (e.g., `schema:variable/biological_sex`)
- `localColumn` — The name of the column in your local data
- `localMappings` (optional) — Maps schema-level term keys to your local values

### Example

```jsonld
"databases": {
  "centre_a_ehr": {
    "@id": "mapping:database/centre_a_ehr",
    "@type": "mapping:Database",
    "name": "Centre A EHR",
    "description": "Main EHR database for Centre A (English)",
    "locale": "en_GB",
    "tables": {
      "patients": {
        "@id": "mapping:table/patients",
        "@type": "mapping:Table",
        "sourceFile": "patients",
        "description": "Patient dataset with 150 records",
        "columns": {
          "biological_sex": {
            "mapsTo": "schema:variable/biological_sex",
            "localColumn": "sex",
            "localMappings": {
              "male": "M",
              "female": "F",
              "missing_or_unspecified": ""
            }
          },
          "age_at_diagnosis": {
            "mapsTo": "schema:variable/age_at_diagnosis",
            "localColumn": "age",
            "localMappings": {
              "missing_or_unspecified": ""
            }
          }
        }
      }
    }
  }
}
```

## Complete Example

A full example variable combining all features (for biological sex):

```jsonld
"biological_sex": {
  "@id": "schema:variable/biological_sex",
  "@type": "schema:CategoricalVariable",
  "dataType": "categorical",
  "predicate": "sio:SIO_000008",
  "class": "ncit:C28421",
  "schemaReconstruction": [
    {
      "@type": "schema:ClassNode",
      "predicate": "sio:SIO_000235",
      "class": "mesh:D000091569",
      "classLabel": "demographicClass",
      "aestheticLabel": "Demographic"
    },
    {
      "@type": "schema:ClassNode",
      "placement": "after",
      "predicate": "sio:SIO_000253",
      "class": "ncit:C142529",
      "classLabel": "ehrClass",
      "aestheticLabel": "EHR"
    }
  ],
  "valueMapping": {
    "terms": {
      "male": {
        "targetClass": "ncit:C20197"
      },
      "female": {
        "targetClass": "ncit:C16576"
      },
      "missing_or_unspecified": {
        "targetClass": "ncit:C54031"
      }
    }
  }
}
```

For complete working examples, see the [Example Data](Example-Data.md) page.
