"""
RDF store service for graph database operations.

This service acts as a facade over the RDF store repository,
providing higher-level business operations.
"""

import logging
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

try:
    from ..repositories import RDFStoreRepository
    from ..repositories.rdf_store_repository import ASK_QUERY_ACCEPT
    from ..utils.rdf_store_url import build_repository_endpoint
except ImportError:
    from repositories import RDFStoreRepository  # type: ignore[no-redef]
    from repositories.rdf_store_repository import (  # type: ignore[no-redef]
        ASK_QUERY_ACCEPT,
    )
    from utils.rdf_store_url import (  # type: ignore[no-redef]
        build_repository_endpoint,
    )

logger = logging.getLogger(__name__)

# The status bar checks the store whenever a page opens: a store that is down
# must not hold the page up for long.
REACHABILITY_TIMEOUT_SECONDS = 3.0


def _in_source_order(columns: Iterable[Tuple[str, Any]]) -> List[str]:
    """
    A table's column names in the order they have in its source.

    The store keeps no order of its own; the ingest records each column's
    1-based place as dbo:column_position. Columns without one (a table
    ingested before positions were recorded) follow the others, in the
    order the store returned them. A column with several positions (its
    table ingested again after the columns moved) is listed once, at its
    lowest.

    Args:
        columns: (name, position) per result row, in the store's order; a
            position the store has none for is empty.
    """
    first_seen: Dict[str, int] = {}
    lowest: Dict[str, int] = {}
    for index, (name, position) in enumerate(columns):
        first_seen.setdefault(name, index)
        place = _as_position(position)
        if place is not None and (name not in lowest or place < lowest[name]):
            lowest[name] = place
    return sorted(
        first_seen,
        key=lambda name: (name not in lowest, lowest.get(name, 0), first_seen[name]),
    )


def _as_position(value: Any) -> Optional[int]:
    """A recorded position as a number, or None when there is none."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


class RDFStoreService:
    """
    Service class for RDF store operations.

    Provides business-level methods for interacting with the RDF store,
    abstracting repository details from controllers.
    """

    def __init__(self, rdf_store_url: str, repo: str):
        """
        Initialise RDF store service.

        Args:
            rdf_store_url: Base URL of the RDF store instance.
            repo: Repository name.
        """
        self.repository = RDFStoreRepository(rdf_store_url, repo)
        self.rdf_store_url = rdf_store_url
        self.repo = repo

    def check_reachable(
        self, timeout: float = REACHABILITY_TIMEOUT_SECONDS
    ) -> Tuple[bool, str]:
        """
        Whether the store answers a query on its repository, and if not, why.

        One ``ASK {}`` with a short timeout and no retries. A 200 counts only
        when it is a SPARQL answer, so a web server that answers every path
        is not taken for a store.

        Returns:
            (True, "") when the store answered; (False, reason) otherwise,
            the reason written for the user.
        """
        endpoint = build_repository_endpoint(self.rdf_store_url, self.repo)
        try:
            response = requests.get(
                endpoint,
                params={"query": "ASK {}"},
                headers={"Accept": ASK_QUERY_ACCEPT},
                timeout=timeout,
            )
        except requests.Timeout:
            return False, f"The store did not answer within {timeout:g} seconds."
        except requests.RequestException as error:
            # The error names the URL, credentials and all: log its kind only.
            logger.info(
                "The store of repository %r cannot be reached (%s)",
                self.repo,
                type(error).__name__,
            )
            return False, "The store cannot be reached from Flyover."
        status = response.status_code
        if status == 404:
            return False, f"The store has no repository named {self.repo!r}."
        if status in (401, 403):
            return False, (
                f"The store refused Flyover's request (HTTP {status}); check "
                "the credentials the endpoint carries."
            )
        if status != 200:
            return False, f"The store answered with HTTP {status}."
        try:
            answered = isinstance(response.json().get("boolean"), bool)
        except (ValueError, AttributeError):
            answered = False
        if not answered:
            return False, "Something answered at that address, but not an RDF store."
        return True, ""

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
            Dict mapping database names to lists of column names, in the
            order the columns have in their source (see _in_source_order).
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

        # Group by database, filtering out None/empty names from regex mismatches
        columns_by_database = {}
        for db in df.get_column("database").unique().to_list():
            if not db:
                continue
            db_columns = df.filter(pl.col("database") == db)
            names = db_columns.get_column("column").to_list()
            positions = (
                db_columns.get_column("position").to_list()
                if "position" in db_columns.columns
                else [None] * len(names)
            )
            columns_by_database[db] = _in_source_order(zip(names, positions))

        return columns_by_database

    def get_categories(
        self, column_name: str, database: Optional[str] = None
    ) -> Optional[str]:
        """
        Get categories for a column.

        Args:
            column_name: Name of the column.
            database: Optional database/table name to scope categories to.

        Returns:
            Categories query result.
        """
        return self.repository.get_categories(column_name, database)

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

    def execute_query(
        self,
        query: str,
        query_type: str = "query",
        endpoint_appendices: str = "",
    ) -> str:
        """
        Execute a SPARQL query on the RDF store.

        Args:
            query: SPARQL query to execute
            query_type: Type of query (query, update, etc.)
            endpoint_appendices: Additional endpoint parameters

        Returns:
            Query result as string

        Raises:
            Exception: If query execution fails or repository returns None
        """
        result = self.repository.execute_query(query, query_type, endpoint_appendices)
        if result is None:
            raise Exception("RDF store query execution failed")
        return result

    def get_existing_column_class_uri(
        self, table_name: str, column_name: str
    ) -> Optional[str]:
        """
        Get existing column class URI from the RDF store.

        Uses the same SPARQL query as get_column_class_uri since both
        new and existing columns share the same URI lookup mechanism.

        Args:
            table_name: Name of the table
            column_name: Name of the column

        Returns:
            Optional[str]: Existing column URI or None if not found
        """
        return self.repository.get_column_class_uri(table_name, column_name)

    def process_cross_graph_relationships(
        self, cross_graph_data: Dict[str, str]
    ) -> Tuple[bool, str]:
        """
        Process cross-graph relationships between tables.

        Delegates to process_cross_graph_relationship (singular) which
        handles URI lookup and relationship insertion via the repository.

        Args:
            cross_graph_data: Dictionary containing relationship data with keys:
                newTableName, newColumnName, existingTableName, existingColumnName

        Returns:
            Tuple of (success, message)
        """
        if not cross_graph_data:
            return True, ""

        try:
            new_table = cross_graph_data["newTableName"]
            new_column = cross_graph_data["newColumnName"]
            existing_table = cross_graph_data["existingTableName"]
            existing_column = cross_graph_data["existingColumnName"]

            result = self.process_cross_graph_relationship(
                new_table, new_column, existing_table, existing_column
            )

            if result:
                return True, (
                    f"Created cross-graph relationship: "
                    f"{new_table}.{new_column} -> "
                    f"{existing_table}.{existing_column}"
                )
            else:
                return False, (
                    f"Failed cross-graph relationship: "
                    f"{new_table}.{new_column} -> "
                    f"{existing_table}.{existing_column}"
                )

        except Exception as e:
            logger.error(f"Failed to process cross-graph relationship: {e}")
            return False, f"Error processing cross-graph relationship: {e}"

    @staticmethod
    def graph_database_find_name_match(
        map_database_name: Optional[str], target_database: str
    ) -> bool:
        """
        Check if a semantic map's database_name matches a target database.

        This function implements strict matching with a fallback for .csv extension handling.
        If map_database_name is None or empty, the map is considered a global template
        and applies to all databases.

        Args:
            map_database_name: The database_name field from the semantic map JSON (can be None)
            target_database: The database name to match against

        Returns:
            bool: True if the map should apply to this database, False otherwise
        """
        # If map_database_name is None or empty, it's a global template - applies to all
        if map_database_name is None or map_database_name == "":
            return True

        # Strict exact match first
        if map_database_name == target_database:
            return True

        # Fallback: try matching with/without .csv extension
        map_name_no_ext = map_database_name.removesuffix(".csv")
        target_no_ext = target_database.removesuffix(".csv")

        if map_name_no_ext == target_no_ext:
            logger.debug(
                f"Database name matched with .csv extension fallback: "
                f"'{map_database_name}' matches '{target_database}'"
            )
            return True

        return False
