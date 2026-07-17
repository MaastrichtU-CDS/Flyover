"""
Query builder module for constructing SPARQL queries.

This module centralises all SPARQL query construction logic,
providing reusable query builders for common operations.

Note on SPARQL Injection:
    This module constructs SPARQL queries using string formatting.
    All inputs should be validated/sanitized before being passed to
    these methods. The sanitize_sparql_value method is provided
    for escaping string literals used in queries.
"""

import logging
import re
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class QueryBuilder:
    """
    Builder class for constructing SPARQL queries.

    Provides methods for building various types of queries used
    throughout the application, including column info, categories,
    relationships, and annotation queries.
    """

    # Default prefixes used across queries
    DEFAULT_PREFIXES = {
        "dbo": "http://um-cds/ontologies/databaseontology/",
        "db": "http://data.local/rdf/ontology/",
        "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
        "owl": "http://www.w3.org/2002/07/owl#",
        "roo": "http://www.cancerdata.org/roo/",
        "ncit": "http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#",
        "sio": "http://semanticscience.org/resource/",
    }

    # Pattern for valid SPARQL identifiers (names, prefixes)
    VALID_IDENTIFIER_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_\-\.]*$")

    @staticmethod
    def sanitize_sparql_value(value: str) -> str:
        """
        Sanitize a string value for safe use in SPARQL queries.

        Escapes special characters that could be used for SPARQL injection.

        Args:
            value: The string value to sanitize.

        Returns:
            str: Sanitized string safe for use in SPARQL queries.
        """
        if value is None:
            return ""
        # Escape backslashes first, then quotes
        value = str(value).replace("\\", "\\\\")
        value = value.replace("'", "\\'")
        value = value.replace('"', '\\"')
        return value

    @classmethod
    def validate_identifier(cls, value: str, field_name: str = "value") -> str:
        """
        Validate that a value is a safe identifier for SPARQL queries.

        Args:
            value: The value to validate.
            field_name: Name of the field for error messages.

        Returns:
            str: The validated value.

        Raises:
            ValueError: If the value contains invalid characters.
        """
        if not value:
            raise ValueError(f"{field_name} cannot be empty")
        if not cls.VALID_IDENTIFIER_PATTERN.match(value):
            raise ValueError(f"{field_name} contains invalid characters: {value}")
        return value

    @classmethod
    def build_prefixes(
        cls, additional_prefixes: Optional[Dict[str, str]] = None
    ) -> str:
        """
        Build PREFIX declarations for SPARQL queries.

        Args:
            additional_prefixes: Optional dict of additional prefixes to include.

        Returns:
            str: Formatted PREFIX declarations.
        """
        prefixes = dict(cls.DEFAULT_PREFIXES)
        if additional_prefixes:
            prefixes.update(additional_prefixes)
        return "\n".join(f"PREFIX {k}: <{v}>" for k, v in prefixes.items())

    @staticmethod
    def column_info_query() -> str:
        """
        Build query to retrieve column information from the graph.

        Returns:
            str: SPARQL SELECT query for column info.
        """
        return """
            PREFIX dbo: <http://um-cds/ontologies/databaseontology/>
            SELECT ?uri ?column
            WHERE {
                ?uri dbo:column ?column .
            }
        """

    @staticmethod
    def database_name_query() -> str:
        """
        Build query to fetch unique database names.

        Returns:
            str: SPARQL SELECT query for database names.
        """
        return """
            PREFIX dbo: <http://um-cds/ontologies/databaseontology/>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            SELECT ?db WHERE {
                ?s dbo:table ?db.
                ?s rdfs:subClassOf dbo:TableRow.
            }
        """

    @staticmethod
    def check_data_graph_exists_query() -> str:
        """
        Build ASK query to check if any data graph exists.

        Returns:
            str: SPARQL ASK query.
        """
        return """
            ASK WHERE {
                GRAPH ?g {
                    ?s ?p ?o
                }
                FILTER(STRSTARTS(STR(?g), "http://data.local/"))
            }
        """

    @classmethod
    def categories_query(
        cls, repo: str, column_name: str, database: Optional[str] = None
    ) -> str:
        """
        Build query to retrieve categories for a column.

        Args:
            repo: Repository name for namespace construction.
            column_name: Name of the column.
            database: Optional database/table name to scope categories to.

        Returns:
            str: SPARQL SELECT query for categories.
        """
        # Sanitize inputs to prevent SPARQL injection
        safe_repo = cls.sanitize_sparql_value(repo)
        safe_column = cls.sanitize_sparql_value(column_name)

        # Add database filter to ensure categories are scoped to the correct table.
        # Use '{database}.' (with trailing dot) to prevent false-positive substring
        # matches (e.g. 'table_a' matching 'table_a_copy'). The column class URI
        # follows the pattern: http://…/{table_name}.column.{column_name}
        db_filter = ""
        if database:
            safe_database = cls.sanitize_sparql_value(database)
            # Strip .csv suffix if present for consistent URI matching
            if safe_database.endswith(".csv"):
                safe_database = safe_database[:-4]
            db_filter = f"FILTER(CONTAINS(LCASE(STR(?v)), LCASE('{safe_database}.')))"

        return f"""
            PREFIX dbo: <http://um-cds/ontologies/databaseontology/>
            PREFIX db: <http://{safe_repo}.local/rdf/ontology/>
            PREFIX roo: <http://www.cancerdata.org/roo/>
            SELECT ?value (COUNT(?value) as ?count)
            WHERE
            {{
               ?a a ?v.
               ?v dbo:column '{safe_column}'.
               {db_filter}
               ?a dbo:has_cell ?cell.
               ?cell dbo:has_value ?value
            }}
            GROUP BY (?value)
        """

    @classmethod
    def column_class_uri_query(cls, table_name: str, column_name: str) -> str:
        """
        Build query to retrieve column class URI.

        Args:
            table_name: Name of the table.
            column_name: Name of the column.

        Returns:
            str: SPARQL SELECT query for column URI.
        """
        # Sanitize inputs to prevent SPARQL injection
        safe_table = cls.sanitize_sparql_value(table_name)
        safe_column = cls.sanitize_sparql_value(column_name)
        # Strip .csv suffix if present for consistent URI matching
        if safe_table.endswith(".csv"):
            safe_table = safe_table[:-4]
        return f"""
            PREFIX dbo: <http://um-cds/ontologies/databaseontology/>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

            SELECT ?uri WHERE {{
                ?uri dbo:column '{safe_column}' .
                FILTER(CONTAINS(LCASE(STR(?uri)), LCASE('{safe_table}.')))
            }}
            LIMIT 1
        """

    @classmethod
    def insert_equivalency_query(
        cls,
        repo: str,
        variable: str,
        database: str,
        var_info_values: List[Any],
    ) -> str:
        """
        Build INSERT query for equivalency triples.

        Args:
            repo: Repository name.
            variable: Variable name.
            database: Database name.
            var_info_values: List of variable info values.

        Returns:
            str: SPARQL INSERT query.
        """
        # Sanitize inputs to prevent SPARQL injection
        safe_repo = cls.sanitize_sparql_value(repo)
        safe_variable = cls.sanitize_sparql_value(variable)
        safe_database = cls.sanitize_sparql_value(database)
        safe_values = cls.sanitize_sparql_value(str(var_info_values))
        ontology_graph = f"http://ontology.local/{safe_database}/"
        return f"""
            PREFIX dbo: <http://um-cds/ontologies/databaseontology/>
            PREFIX db: <http://{safe_repo}.local/rdf/ontology/>
            PREFIX roo: <http://www.cancerdata.org/roo/>
            PREFIX owl: <http://www.w3.org/2002/07/owl#>

            INSERT
            {{
                GRAPH <{ontology_graph}>
                {{ ?s owl:equivalentClass "{safe_values}". }}
            }}
            WHERE
            {{
                ?s dbo:column '{safe_variable}'.
            }}
        """

    @staticmethod
    def fk_relation_insert_query(
        fk_predicate: str,
        column_class_uri: str,
        target_class_uri: str,
        relationships_graph: str = "http://relationships.local/",
    ) -> str:
        """
        Build INSERT query for FK relationship.

        Args:
            fk_predicate: Predicate URI for the relationship.
            column_class_uri: Source column class URI.
            target_class_uri: Target column class URI.
            relationships_graph: Graph URI for relationships.

        Returns:
            str: SPARQL INSERT query.
        """
        return f"""
            PREFIX dbo: <http://um-cds/ontologies/databaseontology/>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

            INSERT {{
                GRAPH <{relationships_graph}> {{
                    ?sources <{fk_predicate}> ?targets .
                }}
            }} WHERE {{
                ?sources rdf:type <{column_class_uri}> ;
                         dbo:has_cell ?sourceCell .
                ?sourceCell dbo:has_value ?columnValue .

                ?targets rdf:type <{target_class_uri}> ;
                         dbo:has_cell ?targetCell .
                ?targetCell dbo:has_value ?columnValue .
            }}
        """

    @staticmethod
    def cross_graph_relation_insert_query(
        predicate: str,
        new_column_uri: str,
        existing_column_uri: str,
        relationships_graph: str = "http://relationships.local/",
    ) -> str:
        """
        Build INSERT query for cross-graph relationship.

        Args:
            predicate: Predicate URI for the relationship.
            new_column_uri: New column class URI.
            existing_column_uri: Existing column class URI.
            relationships_graph: Graph URI for relationships.

        Returns:
            str: SPARQL INSERT query.
        """
        return f"""
            PREFIX dbo: <http://um-cds/ontologies/databaseontology/>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

            INSERT {{
                GRAPH <{relationships_graph}> {{
                    ?newSources <{predicate}> ?existingSources .
                }}
            }} WHERE {{
                ?newSources rdf:type <{new_column_uri}> ;
                            dbo:has_cell ?newCell .
                ?newCell dbo:has_value ?columnValue .

                ?existingSources rdf:type <{existing_column_uri}> ;
                                 dbo:has_cell ?existingCell .
                ?existingCell dbo:has_value ?columnValue .
            }}
        """

    @staticmethod
    def graph_structure_query() -> str:
        """
        Build query to get graph structure for linking.

        Returns:
            str: SPARQL SELECT query for graph structure.
        """
        return """
            PREFIX dbo: <http://um-cds/ontologies/databaseontology/>
            SELECT ?uri ?column
            WHERE {
                ?uri dbo:column ?column .
            }
        """

    @staticmethod
    def ontology_graphs_query() -> str:
        """
        Build query to find all ontology graphs.

        Returns:
            str: SPARQL SELECT query for ontology graphs.
        """
        return """
            SELECT DISTINCT ?g WHERE {
                GRAPH ?g {
                    ?s ?p ?o .
                }
                FILTER(STRSTARTS(STR(?g), "http://ontology.local/"))
            }
        """

    @staticmethod
    def annotation_ask_query(
        database: str,
        local_definition: str,
        var_class: str,
        value_mapping: Optional[Dict] = None,
        prefixes: str = "",
    ) -> str:
        """
        Build ASK query for annotation verification.

        Args:
            database: Database name.
            local_definition: Local column definition.
            var_class: Variable class URI.
            value_mapping: Optional value mapping with terms.
            prefixes: SPARQL prefixes string.

        Returns:
            str: SPARQL ASK query.
        """
        ask_query_parts = []
        data_parts = []

        annotation_graph = f"http://annotation.local/{database}/"

        # Add the main equivalentClass statement
        ask_query_parts.append(
            f"db:{database}.{local_definition} owl:equivalentClass {var_class} ."
        )

        # Check for value mappings - verify the annotation created the specific
        # target class with proper relationships
        if value_mapping and value_mapping.get("terms"):
            for i, (_term, term_info) in enumerate(value_mapping["terms"].items()):
                if term_info.get("local_term") and term_info.get("target_class"):
                    target_class = term_info["target_class"]
                    local_term_value = term_info["local_term"]

                    # Check that the target class is a subclass of the variable class
                    # This verifies the basic ontology relationship for value mapping
                    ask_query_parts.append(
                        f"{target_class} rdfs:subClassOf {var_class} ."
                    )

                    # Also check that the target class has an equivalentClass statement
                    # This verifies the value mapping was created by the annotation
                    ask_query_parts.append(
                        f"{target_class} owl:equivalentClass ?equiv_{i} ."
                    )

                    # Check that there exists at least one cell with this value in the data graph
                    escaped_local_value = local_term_value.replace(
                        "\\", "\\\\"
                    ).replace('"', '\\"')
                    data_parts.append(
                        f'?cell_{i} dbo:has_value "{escaped_local_value}"^^xsd:string .'
                    )

        # Build data graph query part
        data_graph_query = ""
        if data_parts:
            data_graph_query = f' GRAPH ?dataGraph {{ {" ".join(data_parts)} }} FILTER(STRSTARTS(STR(?dataGraph), "http://data.local/"))'

        return f"""
            {prefixes}
            ASK {{
            GRAPH <{annotation_graph}>
             {{
              {' '.join(ask_query_parts)}
             }}{data_graph_query}
            }}
        """
