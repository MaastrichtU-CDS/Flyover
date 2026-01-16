"""
Flyover JSON-LD Mapping Loader

This module provides dataclasses and loaders for working with JSON-LD semantic
mapping files. It offers a clean, typed interface for accessing schema variables,
database configurations, and local mappings.

Classes:
    SchemaReconstructionNode: A node in the schema reconstruction chain.
    SchemaVariable: A variable definition in the semantic schema.
    ColumnMapping: A column mapping linking local data to schema variables.
    Table: A table containing column mappings.
    Database: A database containing tables.
    JSONLDMapping: Complete JSON-LD mapping with schema and databases.

Usage:
    mapping = JSONLDMapping.from_file('mapping.jsonld')
    var = mapping.get_variable('biological_sex')
    target = mapping.get_target_class('biological_sex', 'male')
"""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field


# ============================================================================
# Schema Dataclasses
# ============================================================================


@dataclass
class SchemaReconstructionNode:
    """
    A node in the schema reconstruction chain.

    Represents a step in the RDF graph structure for a variable,
    such as intermediate classes or unit nodes.

    Attributes:
        node_type: Type of node (e.g., "schema:ClassNode", "schema:UnitNode").
        predicate: RDF predicate for this node.
        class_uri: RDF class URI for this node.
        placement: Position directive - "before" or "after" (optional).
        class_label: Label for referencing this class in code (optional).
        node_label: Label for referencing this node in code (optional).
        aesthetic_label: Human-readable display label (optional).
    """

    node_type: str
    predicate: str
    class_uri: str
    placement: Optional[str] = None
    class_label: Optional[str] = None
    node_label: Optional[str] = None
    aesthetic_label: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict) -> "SchemaReconstructionNode":
        """Create a SchemaReconstructionNode from a dictionary."""
        return cls(
            node_type=data.get("@type", "schema:ClassNode"),
            predicate=data.get("predicate", ""),
            class_uri=data.get("class", ""),
            placement=data.get("placement"),
            class_label=data.get("classLabel"),
            node_label=data.get("nodeLabel"),
            aesthetic_label=data.get("aestheticLabel"),
        )

    def to_dict(self) -> dict:
        """Convert to dictionary format."""
        result = {
            "@type": self.node_type,
            "predicate": self.predicate,
            "class": self.class_uri,
        }
        if self.placement:
            result["placement"] = self.placement
        if self.class_label:
            result["classLabel"] = self.class_label
        if self.node_label:
            result["nodeLabel"] = self.node_label
        if self.aesthetic_label:
            result["aestheticLabel"] = self.aesthetic_label
        return result


@dataclass
class SchemaVariable:
    """
    A variable definition in the semantic schema.

    Attributes:
        key: Variable identifier key (e.g., "biological_sex").
        var_id: Full URI identifier for this variable.
        var_type: Variable type (e.g., "schema:CategoricalVariable").
        data_type: Data type - identifier, categorical, continuous, etc.
        predicate: RDF predicate for this variable.
        class_uri: RDF class URI for this variable.
        schema_reconstruction: List of reconstruction nodes (optional).
        value_mappings: Dict mapping term keys to target classes (optional).
    """

    key: str
    var_id: str
    var_type: str
    data_type: str
    predicate: str
    class_uri: str
    schema_reconstruction: List[SchemaReconstructionNode] = field(default_factory=list)
    value_mappings: Dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, key: str, data: dict) -> "SchemaVariable":
        """Create a SchemaVariable from a dictionary."""
        # Parse schema reconstruction
        reconstruction = []
        if "schemaReconstruction" in data:
            for node_data in data["schemaReconstruction"]:
                reconstruction.append(SchemaReconstructionNode.from_dict(node_data))

        # Parse value mappings (extract target classes)
        value_mappings = {}
        if "valueMapping" in data and "terms" in data["valueMapping"]:
            for term_key, term_data in data["valueMapping"]["terms"].items():
                if isinstance(term_data, dict) and "targetClass" in term_data:
                    value_mappings[term_key] = term_data["targetClass"]

        return cls(
            key=key,
            var_id=data.get("@id", f"schema:variable/{key}"),
            var_type=data.get("@type", "schema:CategoricalVariable"),
            data_type=data.get("dataType", "categorical"),
            predicate=data.get("predicate", ""),
            class_uri=data.get("class", ""),
            schema_reconstruction=reconstruction,
            value_mappings=value_mappings,
        )

    def to_dict(self) -> dict:
        """Convert to JSON-LD dictionary format."""
        result = {
            "@id": self.var_id,
            "@type": self.var_type,
            "dataType": self.data_type,
            "predicate": self.predicate,
            "class": self.class_uri,
        }

        if self.schema_reconstruction:
            result["schemaReconstruction"] = [
                node.to_dict() for node in self.schema_reconstruction
            ]

        if self.value_mappings:
            result["valueMapping"] = {
                "terms": {
                    term: {"targetClass": target}
                    for term, target in self.value_mappings.items()
                }
            }

        return result


# ============================================================================
# Database/Mapping Dataclasses
# ============================================================================


@dataclass
class ColumnMapping:
    """
    A column mapping linking local data to schema variables.

    Attributes:
        key: Column identifier key.
        maps_to: Reference to schema variable (e.g., "schema:variable/biological_sex").
        local_column: Local column name in source data.
        local_mappings: Dict mapping schema terms to local values.
    """

    key: str
    maps_to: str
    local_column: str
    local_mappings: Dict[str, Optional[str]] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, key: str, data: dict) -> "ColumnMapping":
        """Create a ColumnMapping from a dictionary."""
        local_col = data.get("localColumn", "")
        if isinstance(local_col, list):
            local_col = local_col[0] if local_col else ""
        return cls(
            key=key,
            maps_to=data.get("mapsTo", ""),
            local_column=local_col,
            local_mappings=data.get("localMappings", {}),
        )

    def get_variable_key(self) -> Optional[str]:
        """Extract the variable key from the mapsTo reference."""
        if self.maps_to and self.maps_to.startswith("schema:variable/"):
            return self.maps_to.replace("schema:variable/", "")
        return None

    def to_dict(self) -> dict:
        """Convert to JSON-LD dictionary format."""
        result = {
            "mapsTo": self.maps_to,
            "localColumn": self.local_column,
        }
        if self.local_mappings:
            result["localMappings"] = self.local_mappings
        return result


@dataclass
class Table:
    """
    A table containing column mappings.

    Attributes:
        key: Table identifier key.
        table_id: Full URI identifier for this table.
        table_type: Type declaration for this table.
        source_file: Source file name or path.
        description: Description of this table.
        columns: Dict of column key to ColumnMapping.
    """

    key: str
    table_id: str
    table_type: str
    source_file: str
    description: str
    columns: Dict[str, ColumnMapping] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, key: str, data: dict) -> "Table":
        """Create a Table from a dictionary."""
        columns = {}
        for col_key, col_data in data.get("columns", {}).items():
            columns[col_key] = ColumnMapping.from_dict(col_key, col_data)

        return cls(
            key=key,
            table_id=data.get("@id", f"mapping:table/{key}"),
            table_type=data.get("@type", "mapping:Table"),
            source_file=data.get("sourceFile", ""),
            description=data.get("description", ""),
            columns=columns,
        )

    def to_dict(self) -> dict:
        """Convert to JSON-LD dictionary format."""
        return {
            "@id": self.table_id,
            "@type": self.table_type,
            "sourceFile": self.source_file,
            "description": self.description,
            "columns": {key: col.to_dict() for key, col in self.columns.items()},
        }


@dataclass
class Database:
    """
    A database containing tables.

    Attributes:
        key: Database identifier key.
        db_id: Full URI identifier for this database.
        db_type: Type declaration for this database.
        name: Human-readable name for this database.
        description: Description of this database.
        locale: Locale code for this database (e.g., en_GB, nl_NL).
        endpoint: Optional database-specific endpoint override.
        tables: Dict of table key to Table.
    """

    key: str
    db_id: str
    db_type: str
    name: str
    description: str
    locale: Optional[str] = None
    endpoint: Optional[str] = None
    tables: Dict[str, Table] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, key: str, data: dict) -> "Database":
        """Create a Database from a dictionary."""
        tables = {}
        for table_key, table_data in data.get("tables", {}).items():
            tables[table_key] = Table.from_dict(table_key, table_data)

        return cls(
            key=key,
            db_id=data.get("@id", f"mapping:database/{key}"),
            db_type=data.get("@type", "mapping:Database"),
            name=data.get("name", key),
            description=data.get("description", ""),
            locale=data.get("locale"),
            endpoint=data.get("endpoint"),
            tables=tables,
        )

    def to_dict(self) -> dict:
        """Convert to JSON-LD dictionary format."""
        result = {
            "@id": self.db_id,
            "@type": self.db_type,
            "name": self.name,
            "description": self.description,
            "tables": {key: table.to_dict() for key, table in self.tables.items()},
        }
        if self.locale:
            result["locale"] = self.locale
        if self.endpoint:
            result["endpoint"] = self.endpoint
        return result


# ============================================================================
# Main JSONLDMapping Class
# ============================================================================


@dataclass
class JSONLDMapping:
    """
    Complete JSON-LD mapping with schema and databases.

    This is the main class for working with Flyover semantic mapping files.
    It provides accessors for schema variables, databases, and local mappings.

    Attributes:
        mapping_id: URI identifier for this mapping.
        mapping_type: Type declaration for this mapping.
        name: Human-readable name for this mapping.
        description: Description of this mapping.
        version: Version string (e.g., "1.0.0").
        created: Creation date string.
        endpoint: SPARQL endpoint URL.
        context: JSON-LD context dictionary.
        schema_id: URI identifier for the schema.
        schema_name: Name of the schema.
        schema_version: Version of the schema.
        prefixes: Namespace prefix definitions.
        variables: Dict of variable key to SchemaVariable.
        databases: Dict of database key to Database.
        raw_data: Original raw dictionary data.
    """

    mapping_id: str
    mapping_type: str
    name: str
    description: str
    version: str
    created: str
    endpoint: str
    context: Dict[str, Any]
    schema_id: str
    schema_name: str
    schema_version: str
    prefixes: Dict[str, str]
    variables: Dict[str, SchemaVariable]
    databases: Dict[str, Database]
    raw_data: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict) -> "JSONLDMapping":
        """
        Create a JSONLDMapping from a dictionary.

        Args:
            data: Raw JSON-LD mapping data as a dictionary.

        Returns:
            Populated JSONLDMapping instance.
        """
        # Parse schema
        schema_data = data.get("schema", {})

        # Parse variables
        variables = {}
        for var_key, var_data in schema_data.get("variables", {}).items():
            variables[var_key] = SchemaVariable.from_dict(var_key, var_data)

        # Parse databases
        databases = {}
        for db_key, db_data in data.get("databases", {}).items():
            databases[db_key] = Database.from_dict(db_key, db_data)

        return cls(
            mapping_id=data.get("@id", ""),
            mapping_type=data.get("@type", "mapping:DataMapping"),
            name=data.get("name", ""),
            description=data.get("description", ""),
            version=data.get("version", "1.0.0"),
            created=data.get("created", ""),
            endpoint=data.get("endpoint", ""),
            context=data.get("@context", {}),
            schema_id=schema_data.get("@id", ""),
            schema_name=schema_data.get("name", ""),
            schema_version=schema_data.get("version", "1.0.0"),
            prefixes=schema_data.get("prefixes", {}),
            variables=variables,
            databases=databases,
            raw_data=data,
        )

    @classmethod
    def from_file(cls, file_path: Union[str, Path]) -> "JSONLDMapping":
        """
        Load a JSONLDMapping from a file.

        Args:
            file_path: Path to the JSON-LD mapping file.

        Returns:
            Populated JSONLDMapping instance.

        Raises:
            FileNotFoundError: If the file does not exist.
            json.JSONDecodeError: If the file is not valid JSON.
        """
        file_path = Path(file_path)
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return cls.from_dict(data)

    # ========================================================================
    # Accessor Methods
    # ========================================================================

    def get_variable(self, variable_key: str) -> Optional[SchemaVariable]:
        """
        Get a schema variable by its key.

        Args:
            variable_key: The variable identifier (e.g., "biological_sex").

        Returns:
            SchemaVariable if found, None otherwise.
        """
        return self.variables.get(variable_key)

    def get_database(self, database_key: str) -> Optional[Database]:
        """
        Get a database by its key.

        Args:
            database_key: The database identifier.

        Returns:
            Database if found, None otherwise.
        """
        return self.databases.get(database_key)

    def get_all_tables(self) -> List[Table]:
        """
        Get all tables from all databases.

        Returns:
            List of all Table objects across all databases.
        """
        tables = []
        for database in self.databases.values():
            tables.extend(database.tables.values())
        return tables

    def get_column_for_variable(
        self,
        variable_key: str,
        database_key: Optional[str] = None,
        table_key: Optional[str] = None,
    ) -> Optional[ColumnMapping]:
        """
        Get the column mapping for a specific variable.

        Args:
            variable_key: The variable identifier.
            database_key: Optional database to search in.
            table_key: Optional table to search in.

        Returns:
            ColumnMapping if found, None otherwise.
        """
        target_maps_to = f"schema:variable/{variable_key}"

        # Filter databases
        dbs = self.databases.values()
        if database_key:
            db = self.databases.get(database_key)
            dbs = [db] if db else []

        for database in dbs:
            # Filter tables
            tables = database.tables.values()
            if table_key:
                table = database.tables.get(table_key)
                tables = [table] if table else []

            for table in tables:
                for column in table.columns.values():
                    if column.maps_to == target_maps_to:
                        return column

        return None

    def get_local_term(
        self,
        variable_key: str,
        schema_term: str,
        database_key: Optional[str] = None,
        table_key: Optional[str] = None,
    ) -> Optional[str]:
        """
        Get the local term for a schema term.

        Args:
            variable_key: The variable identifier.
            schema_term: The schema term (e.g., "male", "female").
            database_key: Optional database to search in.
            table_key: Optional table to search in.

        Returns:
            Local term string if found, None otherwise.
        """
        column = self.get_column_for_variable(variable_key, database_key, table_key)
        if column and schema_term in column.local_mappings:
            return column.local_mappings[schema_term]
        return None

    def get_target_class(self, variable_key: str, schema_term: str) -> Optional[str]:
        """
        Get the target ontology class for a schema term.

        Args:
            variable_key: The variable identifier.
            schema_term: The schema term (e.g., "male", "female").

        Returns:
            Target class URI string if found, None otherwise.
        """
        variable = self.get_variable(variable_key)
        if variable and schema_term in variable.value_mappings:
            return variable.value_mappings[schema_term]
        return None

    def get_local_column(
        self,
        variable_key: str,
        database_key: Optional[str] = None,
        table_key: Optional[str] = None,
    ) -> Optional[str]:
        """
        Get the local column name for a variable.

        Args:
            variable_key: The variable identifier.
            database_key: Optional database to search in.
            table_key: Optional table to search in.

        Returns:
            Local column name if found, None otherwise.
        """
        column = self.get_column_for_variable(variable_key, database_key, table_key)
        return column.local_column if column else None

    def get_all_variable_keys(self) -> list:
        """
        Get list of all variable keys in the schema.

        Returns:
            List of variable key strings.
        """
        return list(self.variables.keys())

    def has_variable(self, variable_key: str) -> bool:
        """
        Check if a variable exists in the schema.

        Args:
            variable_key: The variable identifier.

        Returns:
            True if variable exists, False otherwise.
        """
        return variable_key in self.variables

    def get_first_database_name(self) -> Optional[str]:
        """
        Get the name of the first database (for backwards compatibility).

        Returns:
            Name of first database or None if no databases.
        """
        if self.databases:
            first_db = list(self.databases.values())[0]
            return first_db.name
        return None

    def find_database_key_for_graphdb(self, graphdb_name: str) -> Optional[str]:
        """
        Find the JSON-LD database key that matches a GraphDB database name.

        Matches by checking database name or table sourceFile against the GraphDB name.
        Handles .csv extension differences.

        Args:
            graphdb_name: The database name from GraphDB.

        Returns:
            The matching database key, or None if no match found.
        """
        if not self.databases:
            return None

        graphdb_no_ext = (
            graphdb_name[:-4] if graphdb_name.endswith(".csv") else graphdb_name
        )

        for db_key, db in self.databases.items():
            db_name_no_ext = db.name[:-4] if db.name.endswith(".csv") else db.name
            if db.name == graphdb_name or db_name_no_ext == graphdb_no_ext:
                return db_key

            for table in db.tables.values():
                source_no_ext = (
                    table.source_file[:-4]
                    if table.source_file.endswith(".csv")
                    else table.source_file
                )
                if table.source_file == graphdb_name or source_no_ext == graphdb_no_ext:
                    return db_key

        return None

    # ========================================================================
    # Conversion Methods
    # ========================================================================

    def to_dict(self) -> dict:
        """
        Convert to JSON-LD dictionary format.

        Returns:
            Complete JSON-LD mapping as a dictionary.
        """
        return {
            "@context": self.context,
            "@id": self.mapping_id,
            "@type": self.mapping_type,
            "name": self.name,
            "description": self.description,
            "version": self.version,
            "created": self.created,
            "endpoint": self.endpoint,
            "schema": {
                "@id": self.schema_id,
                "@type": "schema:SemanticSchema",
                "name": self.schema_name,
                "version": self.schema_version,
                "prefixes": self.prefixes,
                "variables": {
                    key: var.to_dict() for key, var in self.variables.items()
                },
            },
            "databases": {key: db.to_dict() for key, db in self.databases.items()},
        }

    def save(self, file_path: Union[str, Path], indent: int = 2) -> None:
        """
        Save the mapping to a JSON-LD file.

        Args:
            file_path: Path to save the file.
            indent: JSON indentation level (default: 2).
        """
        file_path = Path(file_path)
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, indent=indent, ensure_ascii=False)

    def to_legacy_format(
        self,
        database_key: Optional[str] = None,
        table_key: Optional[str] = None,
    ) -> dict:
        """
        Convert JSON-LD mapping to legacy format for annotation helper compatibility.

        This method converts the new JSON-LD structure to the legacy format that is
        expected by the annotation helper's add_annotation function. The legacy format
        uses 'variable_info' with 'local_definition', 'predicate', 'class', 'data_type',
        'schema_reconstruction', and 'value_mapping' with 'terms' containing 'local_term'
        and 'target_class'.

        .. deprecated:: 3.0.0
            This method is transitional and scheduled for removal in version 4.0.0.
            The annotation helper will be updated to work directly with JSON-LD format.

        Args:
            database_key: Optional database key to filter local mappings.
                         If None, uses the first database.
            table_key: Optional table key to filter local mappings.
                      If None, searches all tables.

        Returns:
            Dictionary in legacy format compatible with annotation helper.
        """
        # Build prefixes string in legacy format
        prefixes_str = ""
        for prefix_name, prefix_uri in self.prefixes.items():
            prefixes_str += f"PREFIX {prefix_name}: <{prefix_uri}>\n"

        # Determine database name for legacy format
        db_name = ""
        if database_key and database_key in self.databases:
            db_name = self.databases[database_key].name
        elif self.databases:
            first_db = list(self.databases.values())[0]
            db_name = first_db.name

        legacy = {
            "endpoint": self.endpoint,
            "database_name": db_name,
            "prefixes": prefixes_str.strip(),
            "variable_info": {},
        }

        # Convert each variable to legacy format
        for var_key, var in self.variables.items():
            var_legacy = {
                "predicate": var.predicate,
                "class": var.class_uri,
                "data_type": var.data_type,
                "local_definition": None,  # Will be populated from column mapping
            }

            # Convert schema reconstruction if present
            if var.schema_reconstruction:
                var_legacy["schema_reconstruction"] = []
                for node in var.schema_reconstruction:
                    # Determine node type from the @type field
                    # ClassNode -> "class", UnitNode/PropertyNode -> "node"
                    node_type_value = "class"
                    if node.node_type in ("schema:UnitNode", "schema:PropertyNode"):
                        node_type_value = "node"
                    elif node.node_type != "schema:ClassNode":
                        # For unknown types, default to "class" if class_label exists, otherwise "node"
                        node_type_value = "class" if node.class_label else "node"

                    node_legacy = {
                        "type": node_type_value,
                        "predicate": node.predicate,
                        "class": node.class_uri,
                    }
                    if node.placement:
                        node_legacy["placement"] = node.placement
                    if node.class_label:
                        node_legacy["class_label"] = node.class_label
                    if node.node_label:
                        node_legacy["node_label"] = node.node_label
                    if node.aesthetic_label:
                        node_legacy["aesthetic_label"] = node.aesthetic_label
                    var_legacy["schema_reconstruction"].append(node_legacy)

            # Convert value mapping if present
            if var.value_mappings:
                var_legacy["value_mapping"] = {"terms": {}}
                for term_key, target_class in var.value_mappings.items():
                    var_legacy["value_mapping"]["terms"][term_key] = {
                        "target_class": target_class,
                        "local_term": None,  # Will be populated from column mapping
                    }

            # Get local column and local mappings from database/table
            column = self.get_column_for_variable(var_key, database_key, table_key)
            if column:
                local_col = column.local_column
                if isinstance(local_col, list):
                    local_col = local_col[0] if local_col else None
                var_legacy["local_definition"] = local_col or None

                # Populate local_term values from column mappings
                if "value_mapping" in var_legacy and column.local_mappings:
                    for term_key in var_legacy["value_mapping"]["terms"]:
                        if term_key in column.local_mappings:
                            local_term_value = column.local_mappings[term_key]
                            if isinstance(local_term_value, list):
                                local_term_value = (
                                    local_term_value[0] if local_term_value else None
                                )
                            var_legacy["value_mapping"]["terms"][term_key][
                                "local_term"
                            ] = local_term_value

            legacy["variable_info"][var_key] = var_legacy

        return legacy

    def get_prefixes_string(self) -> str:
        """
        Get prefixes as a SPARQL PREFIX declaration string.

        Returns:
            String with PREFIX declarations for use in SPARQL queries.
        """
        prefixes_str = ""
        for prefix_name, prefix_uri in self.prefixes.items():
            prefixes_str += f"PREFIX {prefix_name}: <{prefix_uri}>\n"
        return prefixes_str.strip()
