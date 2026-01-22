import logging
import os
import sys

from .src.miscellaneous import read_file, add_annotation


def is_jsonld_file(file_path, content):
    """
    Check if the file is a JSON-LD file based on extension or content.

    : param str file_path: path to the file
    :param dict content: parsed content of the file
    :return: True if JSON-LD, False otherwise
    """
    # Check file extension
    if file_path.lower().endswith(".jsonld"):
        return True
    # Check for JSON-LD specific keys
    if isinstance(content, dict) and "@context" in content:
        return True
    return False


def extract_schema_variable_name(maps_to):
    """
    Extract the variable name from a mapsTo reference.

    :param str maps_to:  the mapsTo value, e.g., "schema:variable/biological_sex"
    :return: the variable name, e.g., "biological_sex"
    """
    if not isinstance(maps_to, str):
        return None
    # Handle format like "schema:variable/biological_sex"
    if "/" in maps_to:
        return maps_to.split("/")[-1]
    # Handle format like "schema:variable: biological_sex" (alternative)
    if ":" in maps_to:
        parts = maps_to.split(": ")
        return parts[-1]
    return maps_to


def convert_schema_reconstruction(schema_reconstructions):
    """
    Convert JSON-LD schemaReconstruction format to the format expected by add_annotation.

    :param list schema_reconstructions:  list of schema reconstruction objects from JSON-LD
    :return: list of reconstructions in the expected format
    """
    if not isinstance(schema_reconstructions, list):
        return None

    converted = []
    for reconstruction in schema_reconstructions:
        rec_type = reconstruction.get("@type", "")

        if "ClassNode" in rec_type:
            converted.append(
                {
                    "type": "class",
                    "predicate": reconstruction.get("predicate"),
                    "class": reconstruction.get("class"),
                    "class_label": reconstruction.get("classLabel"),
                    "aesthetic_label": reconstruction.get("aestheticLabel"),
                    "placement": reconstruction.get("placement", "before"),
                }
            )
        elif "UnitNode" in rec_type:
            converted.append(
                {
                    "type": "node",
                    "predicate": reconstruction.get("predicate"),
                    "class": reconstruction.get("class"),
                    "node_label": reconstruction.get("nodeLabel"),
                    "aesthetic_label": reconstruction.get("aestheticLabel"),
                }
            )

    return converted if converted else None


def convert_value_mapping(schema_value_mapping, local_mappings):
    """
    Convert JSON-LD valueMapping format to the format expected by add_annotation,
    merging with local mappings.

    :param dict schema_value_mapping: valueMapping from schema with targetClass
    :param dict local_mappings:  localMappings from table column with local values
    :return: dict in the expected format for add_annotation
    """
    if not isinstance(schema_value_mapping, dict):
        return None

    schema_terms = schema_value_mapping.get("terms", {})
    if not schema_terms:
        return None

    converted_terms = {}
    for term_name, term_data in schema_terms.items():
        target_class = term_data.get("targetClass")
        if not target_class:
            continue

        # Get the local term from localMappings if available
        local_term = None
        if isinstance(local_mappings, dict):
            local_term = local_mappings.get(term_name)

        # Only include if we have a local_term (skip terms not mapped locally)
        if local_term is not None:
            converted_terms[term_name] = {
                "target_class": target_class,
                "local_term": local_term if local_term != "" else None,
            }

    return {"terms": converted_terms} if converted_terms else None


def parse_jsonld_for_table(jsonld_content, database_key, table_key):
    """
    Parse JSON-LD content and extract configuration for a specific table.
    Returns endpoint, table_name (from sourceFile), prefixes, and variable_info
    in the format expected by add_annotation.

    :param dict jsonld_content:  parsed JSON-LD content
    :param str database_key:  key of the database in the databases section
    :param str table_key: key of the table in the tables section
    :return: tuple (endpoint, table_name, prefixes, variable_info)
    """
    # Extract endpoint
    endpoint = jsonld_content.get("endpoint")

    # Extract prefixes from schema
    schema = jsonld_content.get("schema", {})
    prefixes_dict = schema.get("prefixes", {})

    # Convert prefixes dict to string format expected by add_annotation
    prefixes_lines = []
    for prefix, uri in prefixes_dict.items():
        prefixes_lines.append(f"PREFIX {prefix}:  <{uri}>")
    prefixes = "\n".join(prefixes_lines)

    # Extract schema variables
    schema_variables = schema.get("variables", {})

    # Get the specific table
    databases = jsonld_content.get("databases", {})
    database = databases.get(database_key, {})
    tables = database.get("tables", {})
    table = tables.get(table_key, {})

    # Get table name from sourceFile
    table_name = table.get("sourceFile")

    # Get columns
    columns = table.get("columns", {})

    # Build variable_info by merging schema and local mappings
    variable_info = {}

    for column_key, column_data in columns.items():
        maps_to = column_data.get("mapsTo")
        schema_var_name = extract_schema_variable_name(maps_to)

        if not schema_var_name or schema_var_name not in schema_variables:
            logging.warning(
                f"Column '{column_key}' maps to unknown schema variable:  {maps_to}"
            )
            continue

        schema_var = schema_variables[schema_var_name]

        # Get local column - use first item if it's a list
        local_column = column_data.get("localColumn")
        if isinstance(local_column, list) and len(local_column) > 0:
            local_definition = local_column[0]
        elif isinstance(local_column, str):
            local_definition = local_column
        else:
            logging.warning(
                f"Column '{column_key}' has no valid localColumn defined, skipping."
            )
            continue

        # Build the variable info entry
        var_entry = {
            "predicate": schema_var.get("predicate"),
            "class": schema_var.get("class"),
            "local_definition": local_definition,
        }

        # Convert and add schema_reconstruction if present
        schema_reconstruction = convert_schema_reconstruction(
            schema_var.get("schemaReconstruction")
        )
        if schema_reconstruction:
            var_entry["schema_reconstruction"] = schema_reconstruction

        # Convert and add value_mapping if present
        local_mappings = column_data.get("localMappings")
        value_mapping = convert_value_mapping(
            schema_var.get("valueMapping"), local_mappings
        )
        if value_mapping:
            var_entry["value_mapping"] = value_mapping

        # Use the schema variable name as the key (for consistency)
        variable_info[schema_var_name] = var_entry

    return endpoint, table_name, prefixes, variable_info


def get_all_tables(jsonld_content):
    """
    Get all database and table keys from the JSON-LD content.

    :param dict jsonld_content:  parsed JSON-LD content
    :return: list of tuples (database_key, table_key)
    """
    tables_list = []
    databases = jsonld_content.get("databases", {})

    for db_key, db_data in databases.items():
        tables = db_data.get("tables", {})
        for table_key in tables.keys():
            tables_list.append((db_key, table_key))

    return tables_list


def main():
    """
    Main function of adding annotations to a SPARQL endpoint.
    Endpoint, database and annotation data are specified in the settings.

    Supports both legacy JSON format and new JSON-LD format.

    An example of the necessary settings file (legacy JSON) is:
    {
      "endpoint": "http://localhost:7200/repositories/userRepo/statements",
      "database_name": "my_database",
      "biological_sex": {
        "global_variable_name_for_readability": {
          "predicate": "roo: P100018",
          "class": "ncit:C28421",
          "local_definition": "local_variable_name"
        }
      }
    }

    For JSON-LD format, see the schema documentation.
    """
    # check for command-line arguments
    if len(sys.argv) < 2:
        json_file_path = input(
            "Please provide the path to your annotation settings JSON/JSON-LD file:\n"
        )
    else:
        json_file_path = sys.argv[1]

    # retrieve the path, assuming the contents of the settings are located there or in a subdirectory
    path = os.path.dirname(json_file_path)

    # set up logging
    logging.basicConfig(
        filename=f"{os.path.join(path, 'annotation_log.txt')}",
        filemode="a",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    # read the settings
    settings_content = read_file(file_name=json_file_path)

    # Check if this is a JSON-LD file
    if is_jsonld_file(json_file_path, settings_content):
        logging.info("Detected JSON-LD format, using JSON-LD parser.")

        # Get all tables to process
        tables_to_process = get_all_tables(settings_content)

        if not tables_to_process:
            logging.error("No tables found in JSON-LD file, exiting.")
            sys.exit(1)

        logging.info(f"Found {len(tables_to_process)} table(s) to process.")

        # Process each table
        for db_key, table_key in tables_to_process:
            logging.info(f"Processing table '{table_key}' in database '{db_key}'...")

            endpoint, table_name, prefixes, variable_info = parse_jsonld_for_table(
                settings_content, db_key, table_key
            )

            # Validate extracted data
            if not isinstance(endpoint, str) or len(endpoint) < 1:
                logging.error(
                    f"'endpoint' not found or invalid for table '{table_key}', skipping."
                )
                continue

            if not isinstance(table_name, str) or len(table_name) < 1:
                logging.error(
                    f"'sourceFile' (table name) not found or invalid for table '{table_key}', skipping."
                )
                continue

            if not isinstance(prefixes, str) or len(prefixes) < 1:
                logging.warning(
                    f"Prefixes are empty for table '{table_key}', "
                    "default prefixes are limited to:  'db', 'dbo', 'rdf', 'owl', 'roo', and 'ncit'."
                )

            if not variable_info:
                logging.warning(
                    f"No variables found for table '{table_key}', skipping."
                )
                continue

            # Run annotation for this table
            add_annotation(
                endpoint=endpoint,
                database=table_name,
                prefixes=prefixes,
                annotation_data=variable_info,
                path=path,
            )

            logging.info(f"Completed processing table '{table_key}'.")

        logging.info("Completed processing all tables from JSON-LD file.")

    else:
        # Legacy JSON format handling
        logging.info("Using legacy JSON format parser.")

        # check for 'endpoint' key existence in settings
        endpoint = settings_content.get("endpoint")
        if isinstance(endpoint, str) is False:
            logging.error(
                "'endpoint' key not found in the settings or not provided as string, exiting."
            )
            sys.exit(1)
        elif len(endpoint) < 1:
            logging.error("Endpoint is empty, exiting.")
            sys.exit(1)

        # check for 'database_name' key existence in settings
        database = settings_content.get("database_name")
        if isinstance(database, str) is False:
            logging.error(
                "'database' key not found in the settings or not provided as string, exiting."
            )
            sys.exit(1)
        elif len(database) < 1:
            logging.error("Database name is empty, exiting.")
            sys.exit(1)

        prefixes = settings_content.get("prefixes")
        if isinstance(prefixes, str) is False:
            logging.error(
                "'prefixes' key not found in the settings or not provided as string, exiting."
            )
            sys.exit(1)
        elif len(prefixes) < 1:
            logging.warning(
                "Prefixes are empty, please note that default prefixes are limited to: "
                "'db', 'dbo', 'rdf', 'owl', and 'ncit'."
            )
            sys.exit(1)

        # run annotations if specified
        annotations = settings_content.get("variable_info")
        if annotations:
            add_annotation(
                endpoint=endpoint,
                database=database,
                prefixes=prefixes,
                annotation_data=annotations,
                path=path,
            )


if __name__ == "__main__":
    main()
