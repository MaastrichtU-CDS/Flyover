import logging
import os
import sys

from src.miscellaneous import read_file, add_annotation


def main():
    """
    Main function of adding annotations to a SPARQL endpoint.
    Endpoint, database and annotation data are specified in the settings.

    An example of the necessary settings file is:
    {
      "endpoint": "http://localhost:7200/repositories/userRepo/statements",
      "database_name": "my_database",
      "biological_sex": {
        "global_variable_name_for_readability": {
          "predicate": "roo:P100018",
          "class": "ncit:C28421",
          "local_definition": "local_variable_name"
        }
      }
    }
    """
    # check for command-line arguments
    if len(sys.argv) < 2:
        json_file_path = input("Please provide the path to your annotation settings JSON file:\n")
    else:
        json_file_path = sys.argv[1]

    # retrieve the path, assuming the contents of the settings are located there or in a subdirectory
    path = os.path.dirname(json_file_path)

    # set up logging
    logging.basicConfig(filename=f"{os.path.join(path, 'annotation_log.txt')}", filemode='a', level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')

    # read the settings
    settings_content = read_file(file_name=json_file_path)

    # check for 'endpoint' key existence in settings
    endpoint = settings_content.get("endpoint")
    if isinstance(endpoint, str) is False:
        logging.error("'endpoint' key not found in the settings or not provided as string, exiting.")
        sys.exit(1)
    elif len(endpoint) < 1:
        logging.error("Endpoint is empty, exiting.")
        sys.exit(1)

    # check for 'database_name' key existence in settings
    database = settings_content.get("database_name")
    if isinstance(database, str) is False:
        logging.error("'database' key not found in the settings or not provided as string, exiting.")
        sys.exit(1)
    elif len(database) < 1:
        logging.error("Database name is empty, exiting.")
        sys.exit(1)

    prefixes = settings_content.get("prefixes")
    if isinstance(prefixes, str) is False:
        logging.error("'prefixes' key not found in the settings or not provided as string, exiting.")
        sys.exit(1)
    elif len(prefixes) < 1:
        logging.warning("Prefixes are empty, please note that default prefixes are limited to: "
                        "'db', 'dbo', 'rdf', 'owl', 'roo', and 'ncit'.")
        sys.exit(1)

    # run annotations if specified
    annotations = settings_content.get("variable_info")
    if annotations:
        add_annotation(endpoint=endpoint, database=database, prefixes=prefixes, annotation_data=annotations, path=path)


if __name__ == "__main__":
    main()
