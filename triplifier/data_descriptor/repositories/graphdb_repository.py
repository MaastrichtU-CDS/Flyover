"""
GraphDB repository for data access operations.

This module handles all interactions with GraphDB, including
query execution, data retrieval, and graph management.
"""

import logging
from io import StringIO
from typing import Any, Dict, List, Optional, Tuple

import polars as pl
import requests

from .query_builder import QueryBuilder

logger = logging.getLogger(__name__)


class GraphDBRepository:
    """
    Repository class for GraphDB operations.

    Provides methods for executing queries, checking graph existence,
    and managing ontology and data graphs.
    """

    def __init__(self, graphdb_url: str, repo: str):
        """
        Initialise GraphDB repository.

        Args:
            graphdb_url: Base URL of the GraphDB instance.
            repo: Repository name.
        """
        self.graphdb_url = graphdb_url
        self.repo = repo
        self.query_builder = QueryBuilder()

    def _parse_csv_result(self, csv_data: str) -> Optional[pl.DataFrame]:
        """
        Parse CSV query result into a Polars DataFrame.

        Args:
            csv_data: CSV-formatted query result string.

        Returns:
            Polars DataFrame, or None on error.
        """
        if not csv_data or not csv_data.strip():
            return None

        try:
            return pl.read_csv(
                StringIO(csv_data),
                infer_schema_length=0,
                null_values=[],
                try_parse_dates=False,
            )
        except Exception as e:
            logger.error(f"Error parsing CSV result: {e}")
            return None

    def execute_query(
        self,
        query: str,
        query_type: str = "query",
        endpoint_suffix: str = "",
        timeout: int = 30,
    ) -> Optional[str]:
        """
        Execute a SPARQL query on the GraphDB repository.

        Args:
            query: SPARQL query string.
            query_type: Type of query ("query" or "update").
            endpoint_suffix: Additional endpoint path (e.g., "/statements").
            timeout: Request timeout in seconds.

        Returns:
            Query result as string, or None on error.
        """
        try:
            endpoint = f"{self.graphdb_url}/repositories/{self.repo}{endpoint_suffix}"
            response = requests.post(
                endpoint,
                data={query_type: query},
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=timeout,
            )
            return response.text
        except Exception as e:
            logger.error(f"Query execution error: {e}")
            return None

    def execute_ask_query(self, query: str, timeout: int = 30) -> Optional[bool]:
        """
        Execute a SPARQL ASK query.

        Args:
            query: SPARQL ASK query string.
            timeout: Request timeout in seconds.

        Returns:
            Boolean result of ASK query, or None on error.
        """
        try:
            response = requests.get(
                f"{self.graphdb_url}/repositories/{self.repo}",
                params={"query": query},
                headers={"Accept": "application/sparql-results+json"},
                timeout=timeout,
            )
            if response.status_code == 200:
                return response.json().get("boolean")
            return None
        except Exception as e:
            logger.error(f"ASK query execution error: {e}")
            return None

    def check_data_graph_exists(self) -> bool:
        """
        Check if any data graph exists in the repository.

        Returns:
            True if a data graph exists, False otherwise. If the check
            fails due to an error, False is returned.
        """
        query = QueryBuilder.check_data_graph_exists_query()
        try:
            response = requests.get(
                f"{self.graphdb_url}/repositories/{self.repo}",
                params={"query": query},
                headers={"Accept": "application/sparql-results+json"},
                timeout=30,
            )

            if response.status_code == 200:
                return response.json().get("boolean", False)
            else:
                logger.error(f"Query failed with status code {response.status_code}")
                return False
        except Exception as e:
            logger.error(f"Error checking data graph existence: {e}")
            return False

    def get_column_info(self) -> Optional[pl.DataFrame]:
        """
        Retrieve column information from the graph.

        Returns:
            Polars DataFrame with column info, or None on error.
        """
        query = QueryBuilder.column_info_query()
        result = self.execute_query(query)
        return self._parse_csv_result(result)

    def get_database_names(self) -> List[str]:
        """
        Fetch unique database names from the graph.

        Returns:
            List of database names.
        """
        query = QueryBuilder.database_name_query()
        result = self.execute_query(query)
        df = self._parse_csv_result(result)

        if df is None or df.is_empty():
            return []

        try:
            return df.get_column("db").unique().to_list()
        except Exception as e:
            logger.error(f"Error extracting database names: {e}")
            return []

    def get_categories(self, column_name: str) -> Optional[str]:
        """
        Retrieve categories for a column.

        Args:
            column_name: Name of the column.

        Returns:
            Query result as string, or None on error.
        """
        query = QueryBuilder.categories_query(self.repo, column_name)
        return self.execute_query(query)

    def get_column_class_uri(self, table_name: str, column_name: str) -> Optional[str]:
        """
        Retrieve column class URI.

        Args:
            table_name: Name of the table.
            column_name: Name of the column.

        Returns:
            Column URI string, or None if not found.
        """
        query = QueryBuilder.column_class_uri_query(table_name, column_name)
        result = self.execute_query(query)
        df = self._parse_csv_result(result)

        if df is None or df.is_empty():
            logger.debug(f"No results found for column {table_name}.{column_name}")
            return None

        try:
            if "uri" not in df.columns:
                return None
            return df.get_column("uri")[0]
        except Exception as e:
            logger.error(f"Error fetching column URI: {e}")
            return None

    def insert_equivalency(
        self,
        variable: str,
        database: str,
        var_info_values: List[Any],
    ) -> Optional[str]:
        """
        Insert equivalency triple into the ontology graph.

        Args:
            variable: Variable name.
            database: Database name.
            var_info_values: List of variable info values.

        Returns:
            Query result string, or None on error.
        """
        query = QueryBuilder.insert_equivalency_query(
            self.repo, variable, database, var_info_values
        )
        return self.execute_query(query, "update", "/statements")

    def insert_fk_relation(
        self,
        fk_predicate: str,
        column_class_uri: str,
        target_class_uri: str,
        relationships_graph: str = "http://relationships.local/",
    ) -> Optional[str]:
        """
        Insert FK relationship into the relationships graph.

        Args:
            fk_predicate: Predicate URI for the relationship.
            column_class_uri: Source column class URI.
            target_class_uri: Target column class URI.
            relationships_graph: Graph URI for relationships.

        Returns:
            Query result string, or None on error.
        """
        query = QueryBuilder.fk_relation_insert_query(
            fk_predicate, column_class_uri, target_class_uri, relationships_graph
        )
        return self.execute_query(query, "update", "/statements")

    def insert_cross_graph_relation(
        self,
        predicate: str,
        new_column_uri: str,
        existing_column_uri: str,
        relationships_graph: str = "http://relationships.local/",
    ) -> Optional[str]:
        """
        Insert cross-graph relationship.

        Args:
            predicate: Predicate URI for the relationship.
            new_column_uri: New column class URI.
            existing_column_uri: Existing column class URI.
            relationships_graph: Graph URI for relationships.

        Returns:
            Query result string, or None on error.
        """
        query = QueryBuilder.cross_graph_relation_insert_query(
            predicate, new_column_uri, existing_column_uri, relationships_graph
        )
        return self.execute_query(query, "update", "/statements")

    def get_graph_structure(self) -> Dict[str, Any]:
        """
        Get the structure of existing graph data for linking.

        Returns:
            Dict with tables list and tableColumns mapping.
        """
        query = QueryBuilder.graph_structure_query()
        result = self.execute_query(query)
        df = self._parse_csv_result(result)

        if df is None or df.is_empty():
            return {"tables": [], "tableColumns": {}}

        try:
            # Extract table names from URIs
            df = df.with_columns(
                pl.col("uri")
                .str.extract(r".*/(.*?)\.", 1)
                .fill_null("unknown")
                .alias("table")
            )

            tables = df.get_column("table").unique().to_list()
            table_columns = {}
            for table in tables:
                columns = (
                    df.filter(pl.col("table") == table).get_column("column").to_list()
                )
                table_columns[table] = columns

            return {"tables": tables, "tableColumns": table_columns}
        except Exception as e:
            logger.error(f"Error getting graph structure: {e}")
            return {"tables": [], "tableColumns": {}}

    def get_ontology_graphs(self) -> List[str]:
        """
        Find all ontology graphs in the repository.

        Returns:
            List of database names extracted from ontology graph URIs.
        """
        query = QueryBuilder.ontology_graphs_query()
        result = self.execute_query(query)
        df = self._parse_csv_result(result)

        if df is None or df.is_empty():
            return []

        try:
            if "g" not in df.columns:
                return []

            databases = []
            for graph_uri in df.get_column("g").to_list():
                db_name = graph_uri.replace("http://ontology.local/", "").rstrip("/")
                if db_name:
                    databases.append(db_name)
            return databases
        except Exception as e:
            logger.error(f"Error fetching ontology graphs: {e}")
            return []

    def download_ontology(self, graph_uri: str) -> Tuple[Optional[str], int]:
        """
        Download ontology data from a named graph.

        Args:
            graph_uri: URI of the graph to download.

        Returns:
            Tuple of (response text, status code).
        """
        try:
            response = requests.get(
                f"{self.graphdb_url}/repositories/{self.repo}/rdf-graphs/service",
                params={"graph": graph_uri},
                headers={"Accept": "application/n-triples"},
            )
            return response.text, response.status_code
        except Exception as e:
            logger.error(f"Error downloading ontology: {e}")
            return None, 500

    def verify_annotation(
        self,
        database: str,
        local_definition: str,
        var_class: str,
        value_mapping: Optional[Dict] = None,
        additional_prefixes: Optional[Dict[str, str]] = None,
    ) -> Tuple[bool, Optional[bool], Optional[str]]:
        """
        Verify annotation using an ASK query.

        Args:
            database: Database name.
            local_definition: Local column definition.
            var_class: Variable class URI.
            value_mapping: Optional value mapping with terms.
            additional_prefixes: Optional additional prefixes.

        Returns:
            Tuple of (success, is_valid, query_used).
        """
        prefixes = QueryBuilder.build_prefixes(additional_prefixes)
        query = QueryBuilder.annotation_ask_query(
            database, local_definition, var_class, value_mapping, prefixes
        )

        result = self.execute_ask_query(query)
        if result is None:
            return False, None, query

        return True, result, query
