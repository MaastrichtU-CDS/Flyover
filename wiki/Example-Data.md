# Example Data

The `example_data/` folder contains sample datasets and mapping files that demonstrate all supported Flyover features.

## Contents

| File / Folder | Description |
|---|---|
| `mapping_template.jsonld` | A blank JSON-LD mapping template to use as a starting point |
| `centre_a_english/synthetic_english_150.csv` | Synthetic dataset (150 rows, English terminology) |
| `centre_a_english/mapping_centre_a.jsonld` | JSON-LD mapping for Centre A |
| `centre_b_dutch/synthetic_dutch_150.csv` | Synthetic dataset (150 rows, Dutch terminology) |
| `centre_b_dutch/mapping_centre_b.jsonld` | JSON-LD mapping for Centre B |
| `flyover-v3.2.0.mp4` | Video demonstrating the Flyover workflow |

## Two-Centre Scenario

The example data simulates a multi-centre scenario with two fictitious organisations:

- **Centre A** uses English terminology (e.g., `male`, `female`, `oropharynx`)
- **Centre B** uses Dutch terminology (e.g., `man`, `vrouw`, `orofarynx`)

Both centres share the same semantic schema but have different local mappings. This demonstrates how JSON-LD mapping files separate the reusable schema from site-specific terminology.

## What the Example Covers

The example mapping files demonstrate all supported features:

- **Variable types**: identifiers, categorical, and continuous variables
- **Schema reconstruction**: class nodes (with `before` and `after` placement) and unit nodes
- **Value mapping**: mapping local categorical values to ontology classes
- **Local database mappings**: site-specific column names and local terms

## Data Generation

The synthetic datasets were generated using the MOSTLY AI Data Intelligence Platform, guided by the data semantic maps to ensure realistic variable distributions and value ranges.
