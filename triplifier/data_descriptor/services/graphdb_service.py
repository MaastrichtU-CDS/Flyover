"""
GraphDB service for graph database operations.

This service acts as a facade over the GraphDB repository,
providing higher-level business operations.
"""

import logging
from typing import Any, Dict, List, Optional, Tuple

from repositories import GraphDBRepository

logger = logging.getLogger(__name__)


class GraphDBService:
    """
    Service class for GraphDB operations.

    Provides business-level methods for interacting with GraphDB,
    abstracting repository details from controllers.
    """

    def __init__(self, graphdb_url: str, repo: str):
        """
        Initialise GraphDB service.

        Args:
            graphdb_url: Base URL of the GraphDB instance.
            repo: Repository name.
        """
        self.repository = GraphDBRepository(graphdb_url, repo)
        self.graphdb_url = graphdb_url
        self.repo = repo

    def check_data_exists(self) -> bool:
        """
        Check if any data exists in the graph.

        Returns:
            True if data exists, False otherwise.
        """
        try:
            return self.repository.check_data_graph_exists()
        except Exception as e:
            logger.error(f"Error checking data existence: {e}")
            return False

    def get_databases(self) -> List[str]:
        """
        Get list of databases from the graph.

        Returns:
            List of database names.
        """
        return self.repository.get_database_names()

    def get_column_info_by_database(self) -> Dict[str, List[str]]:
        """
        Get columns organised by database.

        Returns:
            Dict mapping database names to lists of column names.
        """
        df = self.repository.get_column_info()
        if df is None:
            return {}

        import polars as pl

        # Extract database from URI
        database_pattern = r".*/(.*?)\."
        df = df.with_columns(
            pl.col("uri").str.extract(database_pattern, 1).alias("database")
        )
        df = df.drop("uri")

        # Group by database
        columns_by_database = {}
        for db in df.get_column("database").unique().to_list():
            db_columns = df.filter(pl.col("database") == db)
            columns_by_database[db] = db_columns.get_column("column").to_list()

        return columns_by_database

    def get_categories(self, column_name: str) -> Optional[str]:
        """
        Get categories for a column.

        Args:
            column_name: Name of the column.

        Returns:
            Categories query result.
        """
        return self.repository.get_categories(column_name)

    def insert_equivalencies(
        self,
        variable: str,
        database: str,
        var_info: Dict[str, Any],
    ) -> bool:
        """
        Insert equivalency triples for a variable.

        Args:
            variable: Variable name.
            database: Database name.
            var_info: Variable info dictionary.

        Returns:
            True if successful, False otherwise.
        """
        # Validate var_info has meaningful content
        if not var_info:
            return False

        type_value = var_info.get("type", "")
        description_value = var_info.get("description", "")
        comments_value = var_info.get("comments", "")

        has_type = type_value not in ["", "Variable type: ", "Variable type: None"]
        has_description = description_value not in [
            "",
            "Variable description: ",
            "Variable description: None",
        ]
        has_comments = comments_value not in [
            "",
            "Variable comment: No comment provided",
        ]

        if not (has_type or has_description or has_comments):
            return False

        result = self.repository.insert_equivalency(
            variable, database, list(var_info.values())
        )
        return result is not None

    def process_pk_fk_relationship(
        self,
        fk_table: str,
        fk_column: str,
        pk_table: str,
        pk_column: str,
    ) -> bool:
        """
        Process a single PK/FK relationship.

        Args:
            fk_table: Foreign key table name.
            fk_column: Foreign key column name.
            pk_table: Primary key table name.
            pk_column: Primary key column name.

        Returns:
            True if successful, False otherwise.
        """
        source_uri = self.repository.get_column_class_uri(fk_table, fk_column)
        target_uri = self.repository.get_column_class_uri(pk_table, pk_column)

        if not source_uri or not target_uri:
            logger.warning(
                f"Could not find URIs for FK: {fk_table}.{fk_column} -> {pk_table}.{pk_column}"
            )
            return False

        fk_predicate = "http://um-cds/ontologies/databaseontology/fk_refers_to"
        result = self.repository.insert_fk_relation(
            fk_predicate, source_uri, target_uri
        )
        return result is not None

    def process_cross_graph_relationship(
        self,
        new_table: str,
        new_column: str,
        existing_table: str,
        existing_column: str,
    ) -> bool:
        """
        Process a cross-graph relationship.

        Args:
            new_table: New table name.
            new_column: New column name.
            existing_table: Existing table name.
            existing_column: Existing column name.

        Returns:
            True if successful, False otherwise.
        """
        new_uri = self.repository.get_column_class_uri(new_table, new_column)
        existing_uri = self.repository.get_column_class_uri(
            existing_table, existing_column
        )

        if not new_uri or not existing_uri:
            logger.warning(
                f"Could not find URIs for cross-graph: "
                f"{new_table}.{new_column} -> {existing_table}.{existing_column}"
            )
            return False

        predicate = "http://um-cds/ontologies/databaseontology/fk_refers_to"
        result = self.repository.insert_cross_graph_relation(
            predicate, new_uri, existing_uri
        )
        return result is not None

    def get_graph_structure(self) -> Dict[str, Any]:
        """
        Get graph structure for linking purposes.

        Returns:
            Dict with tables and column mappings.
        """
        return self.repository.get_graph_structure()

    def download_ontologies(
        self, databases: Optional[List[str]] = None
    ) -> Tuple[List[Tuple[str, str]], List[str]]:
        """
        Download ontologies for specified databases.

        Args:
            databases: List of database names. If None, queries for available graphs.

        Returns:
            Tuple of (list of (filename, content) tuples, list of failed databases).
        """
        if databases is None:
            databases = self.repository.get_ontology_graphs()

        results = []
        failed = []

        for database in databases:
            graph_uri = f"http://ontology.local/{database}/"
            content, status = self.repository.download_ontology(graph_uri)

            if status == 200 and content and content.strip():
                filename = f"local_ontology_{database}.nt"
                results.append((filename, content))
            else:
                failed.append(database)

        return results, failed

    def verify_annotation(
        self,
        database: str,
        local_definition: str,
        var_class: str,
        value_mapping: Optional[Dict] = None,
        additional_prefixes: Optional[Dict[str, str]] = None,
    ) -> Tuple[bool, Optional[bool], Optional[str]]:
        """
        Verify annotation for a variable.

        Args:
            database: Database name.
            local_definition: Local column definition.
            var_class: Variable class URI.
            value_mapping: Optional value mapping.
            additional_prefixes: Optional additional prefixes.

        Returns:
            Tuple of (success, is_valid, query_used).
        """
        return self.repository.verify_annotation(
            database, local_definition, var_class, value_mapping, additional_prefixes
        )
