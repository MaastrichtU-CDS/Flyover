"""
Query builder module for constructing SPARQL queries.

This module centralises all SPARQL query construction logic,
providing reusable query builders for common operations.
"""

import logging
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

    @staticmethod
    def categories_query(repo: str, column_name: str) -> str:
        """
        Build query to retrieve categories for a column.

        Args:
            repo: Repository name for namespace construction.
            column_name: Name of the column.

        Returns:
            str: SPARQL SELECT query for categories.
        """
        return f"""
            PREFIX dbo: <http://um-cds/ontologies/databaseontology/>
            PREFIX db: <http://{repo}.local/rdf/ontology/>
            PREFIX roo: <http://www.cancerdata.org/roo/>
            SELECT ?value (COUNT(?value) as ?count)
            WHERE
            {{
               ?a a ?v.
               ?v dbo:column '{column_name}'.
               ?a dbo:has_cell ?cell.
               ?cell dbo:has_value ?value
            }}
            GROUP BY (?value)
        """

    @staticmethod
    def column_class_uri_query(table_name: str, column_name: str) -> str:
        """
        Build query to retrieve column class URI.

        Args:
            table_name: Name of the table.
            column_name: Name of the column.

        Returns:
            str: SPARQL SELECT query for column URI.
        """
        return f"""
            PREFIX dbo: <http://um-cds/ontologies/databaseontology/>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

            SELECT ?uri WHERE {{
                ?uri dbo:column '{column_name}' .
                FILTER(CONTAINS(LCASE(STR(?uri)), LCASE('{table_name}')))
            }}
            LIMIT 1
        """

    @staticmethod
    def insert_equivalency_query(
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
        ontology_graph = f"http://ontology.local/{database}/"
        return f"""
            PREFIX dbo: <http://um-cds/ontologies/databaseontology/>
            PREFIX db: <http://{repo}.local/rdf/ontology/>
            PREFIX roo: <http://www.cancerdata.org/roo/>
            PREFIX owl: <http://www.w3.org/2002/07/owl#>

            INSERT
            {{
                GRAPH <{ontology_graph}>
                {{ ?s owl:equivalentClass "{var_info_values}". }}
            }}
            WHERE
            {{
                ?s dbo:column '{variable}'.
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

        # Add the main equivalentClass statement
        ask_query_parts.append(
            f"db:{database}.{local_definition} owl:equivalentClass {var_class} ."
        )

        # Check for value mappings and add target_class subClassOf statements
        if value_mapping and value_mapping.get("terms"):
            for _term, term_info in value_mapping["terms"].items():
                if term_info.get("local_term") and term_info.get("target_class"):
                    ask_query_parts.append(
                        f"{term_info['target_class']} rdfs:subClassOf {var_class} ."
                    )

        return f"""
            {prefixes}
            ASK {{
              {' '.join(ask_query_parts)}
            }}
        """
