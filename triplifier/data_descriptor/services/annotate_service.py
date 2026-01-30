"""
Annotate service for annotation operations.

This service handles business logic for semantic annotations,
including annotation execution and verification.
"""

import copy
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class AnnotateService:
    """
    Service class for annotation operations.

    Handles semantic annotation processing, verification,
    and status management.
    """

    @staticmethod
    def get_annotatable_variables(
        variable_info: Dict[str, Any],
        is_jsonld: bool,
        global_semantic_map: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Filter variables that have local definitions for annotation.

        Args:
            variable_info: Dictionary of variable information.
            is_jsonld: Whether the source is JSON-LD format.
            global_semantic_map: Legacy semantic map for fallback.

        Returns:
            Dict of variables with local definitions.
        """
        annotatable = {}

        for var_name, var_data in variable_info.items():
            if is_jsonld:
                has_local_def = var_data.get("local_definition") is not None
                var_copy = copy.deepcopy(var_data)
            else:
                var_copy, has_local_def = (
                    AnnotateService._process_variable_for_annotation(
                        var_name, var_data, global_semantic_map
                    )
                )

            if has_local_def:
                annotatable[var_name] = var_copy

        return annotatable

    @staticmethod
    def _process_variable_for_annotation(
        var_name: str,
        var_data: Dict[str, Any],
        global_semantic_map: Optional[Dict[str, Any]],
    ) -> Tuple[Dict[str, Any], bool]:
        """
        Process a variable for annotation (legacy format support).

        Args:
            var_name: Variable name.
            var_data: Variable data dictionary.
            global_semantic_map: Global semantic map.

        Returns:
            Tuple of (processed var data, has local definition).
        """
        var_copy = copy.deepcopy(var_data)
        has_local_def = var_copy.get("local_definition") is not None

        # Try to get local definition from original JSON if not in formulated map
        if not has_local_def and isinstance(global_semantic_map, dict):
            original_var = global_semantic_map.get("variable_info", {}).get(
                var_name, {}
            )
            if original_var.get("local_definition"):
                var_copy["local_definition"] = original_var["local_definition"]
                has_local_def = True

                # Copy value mappings if needed
                if "value_mapping" in original_var:
                    current_mapping = var_copy.get("value_mapping", {})
                    has_local_terms = False
                    if current_mapping.get("terms"):
                        for term_data in current_mapping["terms"].values():
                            if term_data.get("local_term") is not None:
                                has_local_terms = True
                                break
                    if not has_local_terms:
                        var_copy["value_mapping"] = original_var["value_mapping"]

        return var_copy, has_local_def

    @staticmethod
    def execute_annotation(
        endpoint: str,
        database: str,
        prefixes: str,
        annotated_variables: Dict[str, Any],
        temp_dir: str = "/tmp/annotation_temp",
    ) -> Tuple[bool, Optional[str]]:
        """
        Execute annotation for variables.

        Args:
            endpoint: GraphDB statements endpoint.
            database: Database name.
            prefixes: SPARQL prefixes.
            annotated_variables: Variables to annotate.
            temp_dir: Temporary directory for annotation files.

        Returns:
            Tuple of (success, error_message).
        """
        try:
            from annotation_helper.src.miscellaneous import add_annotation

            os.makedirs(temp_dir, exist_ok=True)

            add_annotation(
                endpoint=endpoint,
                database=database,
                prefixes=prefixes,
                annotation_data=annotated_variables,
                path=temp_dir,
                remove_has_column=False,
                save_query=True,
            )

            return True, None

        except Exception as e:
            logger.error(f"Annotation error for database {database}: {e}")
            return False, str(e)

    @staticmethod
    def prepare_annotation_data(
        databases: List[str],
        session_cache: Any,
        map_table_names: List[str],
        database_name_matcher: Any,
        get_semantic_map_func: Any,
        formulate_local_map_func: Any,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Prepare annotation data for all matching databases.

        Args:
            databases: List of database names.
            session_cache: Session cache object.
            map_table_names: Table names from mapping.
            database_name_matcher: Function to match database names.
            get_semantic_map_func: Function to get semantic map.
            formulate_local_map_func: Function to formulate local map.

        Returns:
            Dict mapping database names to annotation data.
        """
        result = {}

        for database in databases:
            # Check if database matches any table name
            matches = False
            for table_name in map_table_names:
                if database_name_matcher(table_name, database):
                    matches = True
                    break

            # Treat as global template if no table names specified
            if not map_table_names:
                matches = True

            if not matches:
                continue

            # Get database key for JSON-LD
            db_key = None
            if session_cache.jsonld_mapping:
                db_key = session_cache.jsonld_mapping.find_database_key_for_graphdb(
                    database
                )

            # Get semantic map
            semantic_map, _, is_jsonld = get_semantic_map_func(
                session_cache, database_key=db_key
            )

            if semantic_map is None:
                continue

            # Get variable info and prefixes
            if is_jsonld:
                variable_info = semantic_map.get("variable_info", {})
                prefixes = semantic_map.get("prefixes", "")
            else:
                local_map = formulate_local_map_func(database)
                variable_info = local_map.get("variable_info", {})
                prefixes = local_map.get("prefixes", "")

            # Get annotatable variables
            annotatable = AnnotateService.get_annotatable_variables(
                variable_info,
                is_jsonld,
                session_cache.global_semantic_map if not is_jsonld else None,
            )

            if annotatable:
                result[database] = {
                    "variables": annotatable,
                    "prefixes": prefixes,
                    "is_jsonld": is_jsonld,
                }

        return result

    @staticmethod
    def get_verification_data(
        databases: List[str],
        session_cache: Any,
        map_table_names: List[str],
        database_name_matcher: Any,
        get_semantic_map_func: Any,
        formulate_local_map_func: Any,
    ) -> Tuple[List[str], List[str], Dict[str, Any]]:
        """
        Get data for annotation verification page.

        Args:
            databases: List of database names.
            session_cache: Session cache object.
            map_table_names: Table names from mapping.
            database_name_matcher: Function to match database names.
            get_semantic_map_func: Function to get semantic map.
            formulate_local_map_func: Function to formulate local map.

        Returns:
            Tuple of (annotated_variables, unannotated_variables, variable_data).
        """
        annotated = []
        unannotated = []
        variable_data = {}

        for database in databases:
            # Check matching
            matches = False
            for table_name in map_table_names:
                if database_name_matcher(table_name, database):
                    matches = True
                    break
            if not map_table_names:
                matches = True
            if not matches:
                continue

            # Get semantic map
            db_key = None
            if session_cache.jsonld_mapping:
                db_key = session_cache.jsonld_mapping.find_database_key_for_graphdb(
                    database
                )

            semantic_map, _, is_jsonld = get_semantic_map_func(
                session_cache, database_key=db_key
            )

            if is_jsonld:
                variable_info = semantic_map.get("variable_info", {})
                prefixes = semantic_map.get("prefixes", "")
            else:
                local_map = formulate_local_map_func(database)
                variable_info = local_map.get("variable_info", {})
                prefixes = local_map.get("prefixes", "")

            for var_name, var_data in variable_info.items():
                full_name = f"{database}.{var_name}"

                if is_jsonld:
                    has_local_def = var_data.get("local_definition") is not None
                    var_copy = copy.deepcopy(var_data)
                else:
                    var_copy, has_local_def = (
                        AnnotateService._process_variable_for_annotation(
                            var_name, var_data, session_cache.global_semantic_map
                        )
                    )

                if has_local_def:
                    annotated.append(full_name)
                    variable_data[full_name] = {
                        **var_copy,
                        "database": database,
                        "prefixes": prefixes,
                    }
                else:
                    unannotated.append(full_name)

        return annotated, unannotated, variable_data

    @staticmethod
    def verify_single_annotation(
        variable_name: str,
        session_cache: Any,
        graphdb_service: Any,
        get_semantic_map_func: Any,
        formulate_local_map_func: Any,
    ) -> Tuple[bool, bool, Optional[str], Optional[str]]:
        """
        Verify annotation for a single variable.

        Args:
            variable_name: Full variable name (database.variable).
            session_cache: Session cache object.
            graphdb_service: GraphDB service instance.
            get_semantic_map_func: Function to get semantic map.
            formulate_local_map_func: Function to formulate local map.

        Returns:
            Tuple of (success, is_valid, query, error).
        """
        if "." not in variable_name:
            return False, False, None, "Invalid variable format"

        database, var_name = variable_name.split(".", 1)

        # Get semantic map
        db_key = None
        if session_cache.jsonld_mapping:
            db_key = session_cache.jsonld_mapping.find_database_key_for_graphdb(
                database
            )

        semantic_map, _, is_jsonld = get_semantic_map_func(
            session_cache, database_key=db_key
        )

        if semantic_map is None:
            return False, False, None, "No semantic map available"

        # Get variable info
        if is_jsonld:
            variable_info = semantic_map.get("variable_info", {})
        else:
            local_map = formulate_local_map_func(database)
            variable_info = local_map.get("variable_info", {})

        var_data = variable_info.get(var_name)
        if not var_data:
            return False, False, None, "Variable not found"

        var_copy = copy.deepcopy(var_data)

        # Get local definition
        local_def = var_copy.get("local_definition")
        if not local_def and not is_jsonld and session_cache.global_semantic_map:
            original = session_cache.global_semantic_map.get("variable_info", {}).get(
                var_name, {}
            )
            local_def = original.get("local_definition")
            if local_def:
                var_copy["local_definition"] = local_def
                if "value_mapping" in original:
                    var_copy["value_mapping"] = original["value_mapping"]

        var_class = var_copy.get("class")

        if not local_def:
            return False, False, None, "Variable has no local definition"
        if not var_class:
            return False, False, None, "Variable has no class mapping"

        # Clean local definition
        if isinstance(local_def, list):
            local_def = local_def[0] if local_def else ""
        if isinstance(local_def, str):
            local_def = local_def.strip("[]'\"")

        # Get additional prefixes
        additional_prefixes = {}
        if is_jsonld and session_cache.jsonld_mapping:
            additional_prefixes = session_cache.jsonld_mapping.prefixes

        # Execute verification
        success, is_valid, query = graphdb_service.verify_annotation(
            database,
            local_def,
            var_class,
            var_copy.get("value_mapping"),
            additional_prefixes,
        )

        if not success:
            return False, False, query, "Query execution failed"

        return True, is_valid, query, None
