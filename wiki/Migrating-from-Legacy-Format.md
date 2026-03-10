# Migrating from Legacy Format

If you have existing semantic mapping files in the old JSON format (`data_semantic_map.json`), you can convert them to the new JSON-LD format using the bundled conversion script.

## Why Migrate?

Starting with Flyover v3.0.0, the mapping format changed from plain JSON to JSON-LD (`.jsonld`). The new format:

- Separates the reusable **schema** (variable definitions) from **local mappings** (site-specific column names and values)
- Uses standard JSON-LD context for linked data compatibility
- Supports validation against a JSON Schema
- Is required by the current version of Flyover and the Annotation Helper

## Using the Conversion Script

The script is located at `scripts/convert_legacy_mapping.py`.

### Basic Usage

```bash
python scripts/convert_legacy_mapping.py old_mapping.json
```

This creates a converted file named `old_mapping_converted.jsonld` in the same directory.

### Options

```
python scripts/convert_legacy_mapping.py <legacy_file.json> [options]

Options:
  -o, --output          Output file path (default: <input>_converted.jsonld)
  --database-name       Name for the database in the new format
  --table-name          Name for the table in the new format
  --mapping-name        Human-readable name for the mapping
  --mapping-id          URI identifier for the mapping
  --schema-id           URI identifier for the schema
  --dry-run             Show conversion result without writing file
  --validate            Validate output against schema after conversion
```

### Examples

```bash
# Convert with a custom output path
python scripts/convert_legacy_mapping.py old_mapping.json -o new_mapping.jsonld

# Convert with metadata
python scripts/convert_legacy_mapping.py old_mapping.json \
  --database-name my_db \
  --table-name patients \
  --mapping-name "Centre A Mapping"

# Preview the conversion without writing a file
python scripts/convert_legacy_mapping.py old_mapping.json --dry-run

# Convert and validate the output
python scripts/convert_legacy_mapping.py old_mapping.json --validate
```

## What Gets Converted

The conversion script handles the following transformations:

| Old Format | New JSON-LD Format |
|---|---|
| `variable_info` entries | `schema.variables` with proper `@id`, `@type`, and `dataType` |
| `schema_reconstruction` | `schemaReconstruction` with `@type` (`ClassNode` / `UnitNode`) |
| `value_mapping` | `valueMapping` with `targetClass` |
| `local_term` | `localMappings` with `localTerm` in the `databases` section |
| `endpoint` | Top-level `endpoint` field |
| `database_name` | Key in the `databases` object |

## After Converting

1. Review the generated `.jsonld` file to ensure the conversion is correct
2. Fill in any missing fields (e.g., `description`, `endpoint`)
3. Add local database mappings if your original file contained site-specific terms
4. See the [JSON-LD Mapping Format](JSON-LD-Mapping-Format.md) page for the full specification
