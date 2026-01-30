"""
Describe service for variable description operations.

This service handles business logic for variable descriptions,
data types, and metadata management.
"""

import copy
import logging
import re
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class DescribeService:
    """
    Service class for variable description operations.

    Handles variable metadata management, local semantic map
    formulation, and description processing.
    """

    @staticmethod
    def parse_form_data_for_database(
        form_data: Dict[str, str],
        database: str,
        all_databases: List[str],
    ) -> Dict[str, Dict[str, str]]:
        """
        Parse form data to extract variable descriptions for a database.

        Args:
            form_data: Form data dictionary.
            database: Target database name.
            all_databases: List of all database names.

        Returns:
            Dict mapping variable names to their descriptions.
        """
        descriptive_info = {}

        for local_variable_name in form_data:
            # Skip comment fields
            if re.search("^ncit_comment_", local_variable_name):
                continue

            # Find matching database based on prefix
            matching_dbs = [
                db for db in all_databases if local_variable_name.startswith(f"{db}_")
            ]
            if not matching_dbs or max(matching_dbs, key=len) != database:
                continue

            # Remove database prefix
            local_var = local_variable_name.replace(f"{database}_", "")
            form_var_name = f"{database}_{local_var}"

            data_type = form_data.get(form_var_name)
            global_var_name = form_data.get(f"ncit_comment_{form_var_name}")
            comment = form_data.get(f"comment_{form_var_name}")

            descriptive_info[local_var] = {
                "type": f"Variable type: {data_type}",
                "description": f"Variable description: {global_var_name}",
                "comments": f'Variable comment: {comment if comment else "No comment provided"}',
            }

        return descriptive_info

    @staticmethod
    def process_detailed_form_data(
        form_data: Dict[str, str],
        database: str,
        all_databases: List[str],
        existing_info: Dict[str, Dict[str, str]],
    ) -> Dict[str, Dict[str, str]]:
        """
        Process detailed form data (units, categories).

        Args:
            form_data: Form data dictionary.
            database: Target database name.
            all_databases: List of all database names.
            existing_info: Existing descriptive info to update.

        Returns:
            Updated descriptive info dictionary.
        """

        def extract_variable_from_key(key: str, db: str) -> Optional[str]:
            prefix = f"{db}_"
            if "_category_" in key:
                base = key.split("_category_")[0]
            elif "_notation_missing_or_unspecified" in key:
                base = key.split("_notation_missing_or_unspecified")[0]
            else:
                base = key
            if base.startswith(prefix):
                return base[len(prefix) :]
            return None

        # Get keys for this database
        keys = []
        for key in form_data:
            if not key.startswith(f"{database}_"):
                continue
            matching_dbs = [db for db in all_databases if key.startswith(f"{db}_")]
            if matching_dbs and max(matching_dbs, key=len) == database:
                keys.append(key)

        # Extract unique variables
        variables = set()
        for key in keys:
            var = extract_variable_from_key(key, database)
            if var:
                variables.add(var)

        # Process each variable
        for variable in variables:
            if variable not in existing_info:
                existing_info[variable] = {}

            # Get related keys
            var_keys = [
                key
                for key in form_data
                if variable in key
                and not key.startswith("comment_")
                and not key.startswith("count_")
            ]

            for key in var_keys:
                if "_notation_missing_or_unspecified" in key:
                    value = form_data.get(key)
                    existing_info[variable][f"Category: {value}"] = (
                        f"Category {value}: missing_or_unspecified"
                        if value
                        else "No missing value notation provided"
                    )
                elif "_category_" in key and not key.startswith("count_"):
                    category = key.split('_category_"')[1].split('"')[0]
                    count_key = f'count_{database}_{variable}_category_"{category}"'
                    comment_key = f"comment_{key}"

                    existing_info[variable][f"Category: {category}"] = (
                        f"Category {category}: {form_data.get(key)}, comment: "
                        f'{form_data.get(comment_key) or "No comment provided"}, '
                        f'count: {form_data.get(count_key) or "No count available"}'
                    )
                elif "count_" not in key:
                    existing_info[variable]["units"] = (
                        form_data.get(key) or "No units specified"
                    )

        return existing_info

    @staticmethod
    def formulate_local_semantic_map(
        global_semantic_map: Dict[str, Any],
        database: str,
        descriptive_info: Optional[Dict[str, Dict[str, str]]] = None,
    ) -> Dict[str, Any]:
        """
        Formulate a local semantic map for a specific database.

        Args:
            global_semantic_map: The global semantic map template.
            database: Database name.
            descriptive_info: Optional descriptive info from UI.

        Returns:
            Modified semantic map with local definitions.
        """
        modified_map = copy.deepcopy(global_semantic_map)

        # Update database name
        if isinstance(modified_map.get("database_name"), str):
            modified_map["database_name"] = database
        else:
            modified_map["database_name"] = database

        # Reset all local_definitions to null
        for var_name in modified_map.get("variable_info", {}):
            modified_map["variable_info"][var_name]["local_definition"] = None

            # Reset value_mapping local_terms
            var_info = modified_map["variable_info"][var_name]
            if "value_mapping" in var_info and "terms" in var_info["value_mapping"]:
                for term_key in var_info["value_mapping"]["terms"]:
                    var_info["value_mapping"]["terms"][term_key]["local_term"] = None

        # If no descriptive info, return with nulls
        if not descriptive_info:
            return modified_map

        # Process variables from UI
        used_global_variables = {}

        for local_var, local_value in descriptive_info.items():
            if "description" not in local_value or not local_value["description"]:
                continue

            global_var = (
                local_value["description"]
                .split("Variable description: ")[1]
                .lower()
                .replace(" ", "_")
            )

            if global_var not in global_semantic_map.get("variable_info", {}):
                continue

            # Handle duplicates
            if global_var in used_global_variables:
                suffix = used_global_variables[global_var] + 1
                new_global_var = f"{global_var}_{suffix}"
                used_global_variables[global_var] = suffix

                # Create new entry
                modified_map["variable_info"][new_global_var] = copy.deepcopy(
                    global_semantic_map["variable_info"][global_var]
                )
                modified_map["variable_info"][new_global_var]["local_definition"] = None
                modified_map["variable_info"][new_global_var]["data_type"] = None
            else:
                new_global_var = global_var
                used_global_variables[global_var] = 0

            # Set local definition
            modified_map["variable_info"][new_global_var][
                "local_definition"
            ] = local_var

            # Set datatype
            datatype = (
                local_value.get("type", "")
                .split("Variable type: ")[-1]
                .lower()
                .replace(" ", "_")
            )
            if datatype and datatype.strip():
                modified_map["variable_info"][new_global_var]["data_type"] = datatype
            else:
                modified_map["variable_info"][new_global_var]["data_type"] = None

            # Process value mappings
            DescribeService._process_value_mappings(
                modified_map["variable_info"][new_global_var],
                global_semantic_map["variable_info"].get(global_var, {}),
                local_value,
            )

        return modified_map

    @staticmethod
    def _process_value_mappings(
        modified_var: Dict[str, Any],
        original_var: Dict[str, Any],
        local_value: Dict[str, str],
    ) -> None:
        """
        Process value mappings from form data.

        Args:
            modified_var: Variable dict to modify.
            original_var: Original variable definition.
            local_value: Form data for this variable.
        """
        if "value_mapping" not in modified_var:
            return

        original_terms = modified_var["value_mapping"].get("terms", {})
        used_global_terms = {}

        # Reset local_terms first
        for term_key in original_terms:
            original_terms[term_key]["local_term"] = None

        # Update from form data
        for category, value in local_value.items():
            if not category.startswith("Category: ") or not value or not value.strip():
                continue

            global_term = (
                value.split(": ")[1].split(", comment")[0].lower().replace(" ", "_")
            )
            local_term_value = category.split(": ")[1]

            if global_term not in original_terms:
                continue

            # Handle duplicates
            if global_term in used_global_terms:
                suffix = used_global_terms[global_term] + 1
                new_global_term = f"{global_term}_{suffix}"
                used_global_terms[global_term] = suffix

                original_terms[new_global_term] = copy.deepcopy(
                    original_terms[global_term]
                )
                original_terms[new_global_term]["local_term"] = local_term_value
            else:
                original_terms[global_term]["local_term"] = local_term_value
                used_global_terms[global_term] = 0

    @staticmethod
    def get_global_variable_names(
        jsonld_mapping: Optional[Any] = None,
        global_semantic_map: Optional[Dict[str, Any]] = None,
    ) -> List[str]:
        """
        Retrieve global variable names from mapping.

        Args:
            jsonld_mapping: JSON-LD mapping object (preferred).
            global_semantic_map: Legacy semantic map dict.

        Returns:
            List of variable names.
        """
        default_names = [
            "Research subject identifier",
            "Biological sex",
            "Age at inclusion",
            "Other",
        ]

        # Prefer JSON-LD mapping
        if jsonld_mapping is not None:
            try:
                return [
                    name.capitalize().replace("_", " ")
                    for name in jsonld_mapping.get_all_variable_keys()
                ] + ["Other"]
            except Exception as e:
                logger.error(f"Error reading JSON-LD mapping: {e}")
                return default_names

        # Fall back to global_semantic_map
        if not isinstance(global_semantic_map, dict):
            return default_names

        try:
            return [
                name.capitalize().replace("_", " ")
                for name in global_semantic_map.get("variable_info", {}).keys()
            ] + ["Other"]
        except Exception as e:
            logger.error(f"Error reading semantic map: {e}")
            return default_names

    @staticmethod
    def get_preselected_values(
        jsonld_mapping: Any,
        descriptive_info_details: Dict[str, List[Any]],
        databases: List[str],
        database_name_matcher: Any,
    ) -> Dict[str, str]:
        """
        Get preselected values for form fields based on mapping.

        Args:
            jsonld_mapping: JSON-LD mapping object.
            descriptive_info_details: Details for variables.
            databases: List of database names.
            database_name_matcher: Function to match database names.

        Returns:
            Dict mapping form field keys to preselected values.
        """
        if not jsonld_mapping or not descriptive_info_details:
            return {}

        preselected = {}
        map_db_name = jsonld_mapping.get_first_database_name()

        for database, variables in descriptive_info_details.items():
            if not database_name_matcher(map_db_name, database):
                continue

            for variable in variables:
                if not isinstance(variable, dict):
                    continue

                for var_name, categories in variable.items():
                    global_var = var_name.split(" (or")[0].lower().replace(" ", "_")
                    var_info = jsonld_mapping.get_variable(global_var)

                    if not var_info:
                        continue

                    local_column = jsonld_mapping.get_local_column(global_var)

                    for category in categories:
                        cat_value = category.get("value")
                        for term, _target in var_info.value_mappings.items():
                            local_term = jsonld_mapping.get_local_term(global_var, term)
                            if str(local_term) == str(cat_value):
                                key = f'{database}_{local_column or ""}_category_"{cat_value}"'
                                preselected[key] = term.title().replace("_", " ")
                                break

        return preselected
