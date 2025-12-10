import os
import sqlite3
import yaml
import socket
import time
import gc
import logging
from typing import Tuple, Union, List, Dict, Optional
from markupsafe import Markup

from rdflib import Graph, URIRef, RDFS, Namespace

logger = logging.getLogger(__name__)

# Define DBO namespace for PK/FK ontology
DBO = Namespace("http://um-cds/ontologies/databaseontology/")


def _patch_triplifier_columnreference():
    """
    Monkey-patch the triplifier package to fix missing COLUMNREFERENCE constant.

    The triplifier package's ontology_factory.py references COLUMNREFERENCE on line 64
    in the load_ontology() method, but this constant is never imported from the dbo module.

    This patch adds the missing constant to the ontology_factory module before it's used.
    The COLUMNREFERENCE should be DBO.ColumnReference based on the usage context
    (finding foreign key predicates via RDFS.subPropertyOf).

    This is a temporary workaround until the upstream triplifier package is fixed.
    See: https://github.com/MaastrichtU-CDS/triplifier/blob/release/2.0/pythonTool/rdf/ontology_factory.py
    """
    try:
        # Import the dbo module to get the DBO namespace
        from pythonTool.rdf.ontology.dbo import DBO as TRIPLIFIER_DBO

        # Import the ontology_factory module
        import pythonTool.rdf.ontology_factory as ontology_factory_module

        # Define COLUMNREFERENCE as DBO.ColumnReference
        # This is the property that FK predicates are subPropertyOf
        COLUMNREFERENCE = TRIPLIFIER_DBO.ColumnReference

        # Inject the missing constant into the ontology_factory module
        if not hasattr(ontology_factory_module, "COLUMNREFERENCE"):
            ontology_factory_module.COLUMNREFERENCE = COLUMNREFERENCE
            logger.info(
                "Applied monkey-patch: Added COLUMNREFERENCE to ontology_factory module"
            )
        else:
            logger.debug("COLUMNREFERENCE already exists in ontology_factory module")

    except ImportError as e:
        logger.warning(f"Could not apply triplifier monkey-patch: {e}")
    except Exception as e:
        logger.error(f"Error applying triplifier monkey-patch: {e}")


class PkFkVerificationResult:
    """Result of PK/FK verification with debug information."""

    def __init__(
        self,
        success: bool,
        message: str,
        queries_used: Optional[List[str]] = None,
        failed_relationships: Optional[List[Dict]] = None,
    ):
        self.success = success
        self.message = message
        self.queries_used = queries_used or []
        self.failed_relationships = failed_relationships or []


class PythonTriplifierIntegration:
    """Integration layer for the Python Triplifier package."""

    def __init__(self, root_dir="", child_dir="."):
        self.root_dir = root_dir
        self.child_dir = child_dir
        self.hostname = socket.gethostname()

    def _normalize_name(self, name: str) -> str:
        """
        Normalize a table or column name for use in URIs.

        Replaces special characters (spaces, hyphens, .csv extension) with
        underscores to create valid URI components.

        Args:
            name: The table or column name to normalize

        Returns:
            Normalized name suitable for URI construction
        """
        # Remove .csv extension if present
        if name.lower().endswith(".csv"):
            name = name[:-4]
        # Replace special characters with underscores
        return name.replace(" ", "_").replace("-", "_")

    def _get_column_iri(
        self, base_uri: str, table_name: str, column_name: str
    ) -> URIRef:
        """
        Generate the IRI for a column class in the ontology.

        Args:
            base_uri: The base URI for the ontology
            table_name: Name of the table (will be normalized)
            column_name: Name of the column (will be normalized)

        Returns:
            URIRef for the column class
        """
        from urllib.parse import quote

        table_uri = self._normalize_name(table_name)
        column_uri = self._normalize_name(column_name)
        # URL-encode to handle any remaining special characters
        table_uri = quote(table_uri, safe="")
        column_uri = quote(column_uri, safe="")
        return URIRef(f"{base_uri}{table_uri}.{column_uri}")

    def _insert_pk_fk_statements(
        self,
        ontology_path: str,
        base_uri: str,
        pk_fk_data: List[Dict],
    ) -> PkFkVerificationResult:
        """
        Insert PK/FK statements into the ontology file using SPARQL-like INSERT operations.

        Adds the following triples:
        - ?source_iri rdfs:subClassOf dbo:PrimaryKey
        - ?target_iri rdfs:subClassOf dbo:ForeignKey
        - ?source_iri dbo:has_target_column ?target_iri

        Args:
            ontology_path: Path to the ontology file
            base_uri: Base URI for the ontology
            pk_fk_data: List of PK/FK relationship dictionaries from the form

        Returns:
            PkFkVerificationResult with success status and debug info
        """
        queries_used = []
        failed_relationships = []

        try:
            # Load the existing ontology
            graph = Graph()
            graph.parse(ontology_path, format="xml")

            # Bind the DBO namespace
            graph.bind("dbo", DBO)

            # Create mapping of files with their PK/FK info
            file_map = {rel["fileName"]: rel for rel in pk_fk_data}

            # Track inserted relationships for verification
            inserted_count = 0

            for rel in pk_fk_data:
                # Get primary key info
                pk_column = rel.get("primaryKey")
                if pk_column:
                    # Get table name from filename using normalized helper
                    table_name = self._normalize_name(rel["fileName"])
                    pk_iri = self._get_column_iri(base_uri, table_name, pk_column)

                    # Insert: ?pk_iri rdfs:subClassOf dbo:PrimaryKey
                    pk_triple = (pk_iri, RDFS.subClassOf, DBO.PrimaryKey)
                    graph.add(pk_triple)
                    queries_used.append(
                        f"INSERT {{ <{pk_iri}> rdfs:subClassOf dbo:PrimaryKey }}"
                    )
                    logger.info(f"Added PrimaryKey subclass for: {pk_iri}")

                # Get foreign key info
                fk_column = rel.get("foreignKey")
                fk_table = rel.get("foreignKeyTable")
                fk_ref_column = rel.get("foreignKeyColumn")

                if fk_column and fk_table:
                    # Get source (FK) IRI using normalized helper
                    source_table = self._normalize_name(rel["fileName"])
                    fk_iri = self._get_column_iri(base_uri, source_table, fk_column)

                    # Insert: ?fk_iri rdfs:subClassOf dbo:ForeignKey
                    fk_triple = (fk_iri, RDFS.subClassOf, DBO.ForeignKey)
                    graph.add(fk_triple)
                    queries_used.append(
                        f"INSERT {{ <{fk_iri}> rdfs:subClassOf dbo:ForeignKey }}"
                    )
                    logger.info(f"Added ForeignKey subclass for: {fk_iri}")

                    # Find the target table's PK info
                    target_rel = file_map.get(fk_table)
                    if target_rel and target_rel.get("primaryKey"):
                        target_table = self._normalize_name(fk_table)
                        target_pk_column = target_rel["primaryKey"]
                        target_iri = self._get_column_iri(
                            base_uri, target_table, target_pk_column
                        )

                        # Insert: ?fk_iri dbo:has_target_column ?target_iri
                        has_target_triple = (fk_iri, DBO.has_target_column, target_iri)
                        graph.add(has_target_triple)
                        queries_used.append(
                            f"INSERT {{ <{fk_iri}> dbo:has_target_column <{target_iri}> }}"
                        )
                        logger.info(
                            f"Added has_target_column: {fk_iri} -> {target_iri}"
                        )
                        inserted_count += 1
                    else:
                        # Log warning but don't fail - referenced table may not have PK selected
                        logger.warning(
                            f"FK relationship incomplete: {rel['fileName']} references {fk_table}, "
                            f"but {fk_table} has no primary key selected"
                        )
                        failed_relationships.append(
                            {
                                "source": rel["fileName"],
                                "target": fk_table,
                                "reason": "Referenced table has no primary key selected",
                            }
                        )

            # Save the modified ontology
            graph.serialize(destination=ontology_path, format="xml")
            logger.info(
                f"Updated ontology saved with {inserted_count} PK/FK relationships"
            )

            if failed_relationships:
                return PkFkVerificationResult(
                    success=True,  # Partial success
                    message=f"PK/FK statements inserted with {len(failed_relationships)} incomplete relationships",
                    queries_used=queries_used,
                    failed_relationships=failed_relationships,
                )

            return PkFkVerificationResult(
                success=True,
                message=f"Successfully inserted PK/FK statements ({inserted_count} relationships)",
                queries_used=queries_used,
            )

        except Exception as e:
            logger.error(f"Error inserting PK/FK statements: {e}")
            import traceback

            traceback.print_exc()
            return PkFkVerificationResult(
                success=False,
                message=f"Error inserting PK/FK statements: {str(e)}",
                queries_used=queries_used,
                failed_relationships=failed_relationships,
            )

    def _verify_pk_fk_statements(
        self,
        ontology_path: str,
        base_uri: str,
        pk_fk_data: List[Dict],
    ) -> PkFkVerificationResult:
        """
        Verify that PK/FK statements were successfully inserted into the ontology.

        Uses ASK-style verification to check if the triples exist.

        Args:
            ontology_path: Path to the ontology file
            base_uri: Base URI for the ontology
            pk_fk_data: List of PK/FK relationship dictionaries

        Returns:
            PkFkVerificationResult with verification status and debug info
        """
        queries_used = []
        failed_relationships = []

        try:
            # Load the ontology
            graph = Graph()
            graph.parse(ontology_path, format="xml")

            # Create mapping of files with their PK/FK info
            file_map = {rel["fileName"]: rel for rel in pk_fk_data}

            for rel in pk_fk_data:
                # Verify primary key
                pk_column = rel.get("primaryKey")
                if pk_column:
                    table_name = self._normalize_name(rel["fileName"])
                    pk_iri = self._get_column_iri(base_uri, table_name, pk_column)

                    # Check if PK statement exists
                    ask_query = f"ASK {{ <{pk_iri}> rdfs:subClassOf dbo:PrimaryKey }}"
                    queries_used.append(ask_query)

                    pk_triple = (pk_iri, RDFS.subClassOf, DBO.PrimaryKey)
                    if pk_triple not in graph:
                        failed_relationships.append(
                            {
                                "type": "PrimaryKey",
                                "iri": str(pk_iri),
                                "query": ask_query,
                                "reason": "PrimaryKey subclass statement not found",
                            }
                        )

                # Verify foreign key and relationship
                fk_column = rel.get("foreignKey")
                fk_table = rel.get("foreignKeyTable")

                if fk_column and fk_table:
                    source_table = self._normalize_name(rel["fileName"])
                    fk_iri = self._get_column_iri(base_uri, source_table, fk_column)

                    # Check FK subclass
                    ask_query = f"ASK {{ <{fk_iri}> rdfs:subClassOf dbo:ForeignKey }}"
                    queries_used.append(ask_query)

                    fk_triple = (fk_iri, RDFS.subClassOf, DBO.ForeignKey)
                    if fk_triple not in graph:
                        failed_relationships.append(
                            {
                                "type": "ForeignKey",
                                "iri": str(fk_iri),
                                "query": ask_query,
                                "reason": "ForeignKey subclass statement not found",
                            }
                        )

                    # Check has_target_column relationship
                    target_rel = file_map.get(fk_table)
                    if target_rel and target_rel.get("primaryKey"):
                        target_table = self._normalize_name(fk_table)
                        target_pk_column = target_rel["primaryKey"]
                        target_iri = self._get_column_iri(
                            base_uri, target_table, target_pk_column
                        )

                        ask_query = (
                            f"ASK {{ <{fk_iri}> dbo:has_target_column <{target_iri}> }}"
                        )
                        queries_used.append(ask_query)

                        has_target_triple = (fk_iri, DBO.has_target_column, target_iri)
                        if has_target_triple not in graph:
                            failed_relationships.append(
                                {
                                    "type": "has_target_column",
                                    "source": str(fk_iri),
                                    "target": str(target_iri),
                                    "query": ask_query,
                                    "reason": "has_target_column relationship not found",
                                }
                            )

            if failed_relationships:
                return PkFkVerificationResult(
                    success=False,
                    message=f"PK/FK verification failed: {len(failed_relationships)} relationships not found",
                    queries_used=queries_used,
                    failed_relationships=failed_relationships,
                )

            return PkFkVerificationResult(
                success=True,
                message="All PK/FK statements verified successfully",
                queries_used=queries_used,
            )

        except Exception as e:
            logger.error(f"Error verifying PK/FK statements: {e}")
            return PkFkVerificationResult(
                success=False,
                message=f"Error verifying PK/FK statements: {str(e)}",
                queries_used=queries_used,
                failed_relationships=failed_relationships,
            )

    def run_triplifier_csv(
        self,
        csv_data_list,
        csv_paths,
        base_uri=None,
        pk_fk_data: Optional[List[Dict]] = None,
    ):
        """
        Process CSV data using Python Triplifier API with streamlined PK/FK handling.

        This method implements a two-step triplification process:
        1. Generate only the ontology file first
        2. If PK/FK data is provided, insert PK/FK statements into the ontology
        3. Generate data using the (potentially modified) ontology

        Args:
            csv_data_list: List of pandas DataFrames
            csv_paths: List of CSV file paths
            base_uri: Base URI for RDF generation
            pk_fk_data: Optional list of PK/FK relationship dictionaries from the form

        Returns:
            Tuple[bool, str]: (success, message/error)
        """
        pk_fk_verification_result = None

        try:
            # Apply monkey-patch to fix missing COLUMNREFERENCE constant in triplifier
            # This must be called before importing triplifier modules
            _patch_triplifier_columnreference()

            # Import triplifier modules
            from pythonTool.main_app import run_triplifier

            # Create a temporary SQLite database
            temp_db_path = os.path.join(
                self.root_dir, self.child_dir, "static", "files", "temp_triplifier.db"
            )
            os.makedirs(os.path.dirname(temp_db_path), exist_ok=True)

            # Remove existing temp database
            if os.path.exists(temp_db_path):
                os.remove(temp_db_path)

            # Create SQLite connection and load CSV data
            conn = sqlite3.connect(temp_db_path)

            try:
                for i, (csv_data, csv_path) in enumerate(zip(csv_data_list, csv_paths)):
                    # Derive table name from CSV filename
                    table_name = os.path.splitext(os.path.basename(csv_path))[0]
                    # Clean table name to be SQLite compatible
                    table_name = table_name.replace("-", "_").replace(" ", "_")

                    # Write DataFrame to SQLite
                    csv_data.to_sql(table_name, conn, if_exists="replace", index=False)
                    logger.info(f"Loaded CSV data into SQLite table: {table_name}")

            finally:
                conn.close()

            # Create YAML configuration
            config = {
                "db": {"url": f"sqlite:///{temp_db_path}"},
                "repo.dataUri": (
                    base_uri if base_uri else f"http://{self.hostname}/rdf/data/"
                ),
            }

            config_path = os.path.join(
                self.root_dir, self.child_dir, "triplifier_csv_config.yaml"
            )
            with open(config_path, "w") as f:
                yaml.dump(config, f)

            # Set up file paths - output to root_dir for upload_ontology_then_data compatibility
            ontology_path = os.path.join(self.root_dir, "ontology.owl")
            output_path = os.path.join(self.root_dir, "output.ttl")
            base_uri = base_uri or f"http://{self.hostname}/rdf/ontology/"

            # Check if we have PK/FK data to process
            has_pk_fk = pk_fk_data and any(
                rel.get("primaryKey") or rel.get("foreignKey") for rel in pk_fk_data
            )

            if has_pk_fk:
                # Step 1: Generate only ontology first
                logger.info("Step 1/3: Generating ontology only...")

                class OntologyArgs:
                    def __init__(self):
                        self.config = config_path
                        self.output = output_path
                        self.ontology = ontology_path
                        self.baseuri = base_uri
                        self.ontologyAndOrData = "ontology"  # Ontology only

                run_triplifier(OntologyArgs())
                logger.info(f"Ontology generated: {ontology_path}")

                # Step 2: Insert PK/FK statements into the ontology
                logger.info("Step 2/3: Inserting PK/FK statements into ontology...")
                pk_fk_result = self._insert_pk_fk_statements(
                    ontology_path, base_uri, pk_fk_data
                )

                if not pk_fk_result.success:
                    # Log the queries used for debugging
                    logger.error(f"PK/FK insertion failed: {pk_fk_result.message}")
                    logger.error(f"Queries attempted: {pk_fk_result.queries_used}")
                    # Continue with data generation anyway - ontology still exists
                else:
                    logger.info(pk_fk_result.message)

                # Verify PK/FK statements
                pk_fk_verification_result = self._verify_pk_fk_statements(
                    ontology_path, base_uri, pk_fk_data
                )
                if not pk_fk_verification_result.success:
                    logger.warning(
                        f"PK/FK verification warning: {pk_fk_verification_result.message}"
                    )
                    for failed in pk_fk_verification_result.failed_relationships:
                        logger.warning(f"  Failed: {failed}")
                else:
                    logger.info("PK/FK statements verified successfully")

                # Step 3: Generate data using the modified ontology
                logger.info("Step 3/3: Generating data using modified ontology...")

                class DataArgs:
                    def __init__(self):
                        self.config = config_path
                        self.output = output_path
                        self.ontology = ontology_path
                        self.baseuri = base_uri
                        self.ontologyAndOrData = "data"  # Data only

                run_triplifier(DataArgs())
                logger.info(f"Data generated: {output_path}")
            else:
                # No PK/FK data - use original single-pass approach
                logger.info(
                    "No PK/FK data provided, using single-pass triplification..."
                )

                class Args:
                    def __init__(self):
                        self.config = config_path
                        self.output = output_path
                        self.ontology = ontology_path
                        self.baseuri = base_uri
                        self.ontologyAndOrData = None  # Convert both ontology and data

                run_triplifier(Args())

            logger.info(f"Python Triplifier executed successfully")
            logger.info(f"Generated files: {ontology_path}, {output_path}")

            # Clean up temporary files
            if os.path.exists(config_path):
                os.remove(config_path)
            if os.path.exists(temp_db_path):
                gc.collect()  # Force garbage collection
                time.sleep(0.5)  # Wait for file handles to close

                try:
                    os.remove(temp_db_path)
                except PermissionError as pe:
                    logger.warning(
                        f"Could not delete temp database (will be cleaned up on next run): {pe}"
                    )
                    # Not critical - file will be overwritten on next run

            # Build result message
            result_message = "CSV data triplified successfully using Python Triplifier."
            if pk_fk_verification_result:
                if pk_fk_verification_result.success:
                    result_message += " PK/FK relationships integrated successfully."
                else:
                    result_message += (
                        f" Warning: Some PK/FK relationships may not have been fully "
                        f"integrated. {pk_fk_verification_result.message}"
                    )
                    # Include failed relationships for debugging
                    if pk_fk_verification_result.failed_relationships:
                        logger.warning(
                            f"Failed PK/FK relationships: {pk_fk_verification_result.failed_relationships}"
                        )
                        logger.warning(
                            f"Queries used: {pk_fk_verification_result.queries_used}"
                        )

            return True, result_message

        except Exception as e:
            logger.error(f"Error in CSV triplification: {e}")
            import traceback

            traceback.print_exc()
            return False, f"Error processing CSV data: {str(e)}"

    def run_triplifier_sql(
        self, base_uri=None, db_url=None, db_user=None, db_password=None
    ):
        """
        Process PostgreSQL data using Python Triplifier API directly.

        Args:
            base_uri: Base URI for RDF generation
            db_url: Database connection URL (can be set from environment variable)
            db_user: Database user (can be set from environment variable)
            db_password: Database password (can be set from environment variable)

        Returns:
            Tuple[bool, str]: (success, message/error)
        """
        try:
            # Import triplifier modules
            from pythonTool.main_app import run_triplifier

            # Get database configuration from environment variables if not provided
            if db_url is None:
                db_url = os.getenv("TRIPLIFIER_DB_URL", "postgresql://postgres/opc")
            if db_user is None:
                db_user = os.getenv("TRIPLIFIER_DB_USER", "postgres")
            if db_password is None:
                db_password = os.getenv("TRIPLIFIER_DB_PASSWORD", "postgres")

            # Create YAML configuration dynamically
            config = {
                "db": {"url": db_url},
                "repo.dataUri": (
                    base_uri if base_uri else f"http://{self.hostname}/rdf/data/"
                ),
            }

            # Add user and password if they're not in the URL
            if db_user and "://" in db_url and "@" not in db_url:
                # Insert credentials into URL
                parts = db_url.split("://")
                config["db"]["url"] = f"{parts[0]}://{db_user}:{db_password}@{parts[1]}"

            config_path = os.path.join(
                self.root_dir, self.child_dir, "triplifier_sql_config.yaml"
            )
            with open(config_path, "w") as f:
                yaml.dump(config, f)

            # Set up file paths - output to root_dir for upload_ontology_then_data compatibility
            ontology_path = os.path.join(self.root_dir, "ontology.owl")
            output_path = os.path.join(self.root_dir, "output.ttl")
            base_uri = base_uri or f"http://{self.hostname}/rdf/ontology/"

            # Create arguments object for the triplifier
            class Args:
                def __init__(self):
                    self.config = config_path
                    self.output = output_path
                    self.ontology = ontology_path
                    self.baseuri = base_uri
                    self.ontologyAndOrData = None  # Convert both ontology and data

            args = Args()

            # Run Python Triplifier directly using the API
            run_triplifier(args)

            logger.info(f"Python Triplifier executed successfully")
            logger.info(f"Generated files: {ontology_path}, {output_path}")

            # Clean up temporary config file
            if os.path.exists(config_path):
                os.remove(config_path)

            return (
                True,
                "PostgreSQL data triplified successfully using Python Triplifier.",
            )

        except Exception as e:
            logger.error(f"Error in SQL triplification: {e}")
            import traceback

            traceback.print_exc()
            return False, f"Error processing PostgreSQL data: {str(e)}"


def run_triplifier(
    properties_file=None,
    root_dir="",
    child_dir=".",
    csv_data_list=None,
    csv_paths=None,
    pk_fk_data=None,
):
    """
    Run the Python Triplifier for CSV or SQL data.
    This function is the main entry point for triplification.

    Args:
        properties_file: Legacy parameter for backwards compatibility ('triplifierCSV.properties' or 'triplifierSQL.properties')
        root_dir: Root directory for file operations
        child_dir: Child directory for file operations
        csv_data_list: List of pandas DataFrames (for CSV mode)
        csv_paths: List of CSV file paths (for CSV mode)
        pk_fk_data: Optional list of PK/FK relationship dictionaries from the form

    Returns:
        Tuple[bool, Union[str, Markup]]: (success, message)
    """
    try:
        # Initialize Python Triplifier integration
        triplifier = PythonTriplifierIntegration(root_dir, child_dir)

        if properties_file == "triplifierCSV.properties":
            # Use Python Triplifier for CSV processing with PK/FK handling
            success, message = triplifier.run_triplifier_csv(
                csv_data_list, csv_paths, pk_fk_data=pk_fk_data
            )

        elif properties_file == "triplifierSQL.properties":
            # Use Python Triplifier for PostgreSQL processing
            success, message = triplifier.run_triplifier_sql()
        else:
            return False, f"Unknown properties file: {properties_file}"

        return success, message

    except Exception as e:
        logger.error(f"Unexpected error attempting to run the Python Triplifier: {e}")
        import traceback

        traceback.print_exc()
        return False, f"Unexpected error attempting to run the Triplifier, error: {e}"
