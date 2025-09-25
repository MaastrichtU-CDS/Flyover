from typing import List, Dict
from rdflib import URIRef, Literal, RDF, RDFS, OWL
from .rdf_factory import RdfFactory
from .ontology.dbo import (
    DBO,
    add_ontology_to_graph,
    DATABASETABLE,
    DATABASECOLUMN,
    PRIMARYKEY,
    FOREIGNKEY,
    TABLE,
    CATALOG,
    SCHEMA,
    COLUMN,
)
from ..foreign_key_specification import ForeignKeySpecification


class OntologyFactory(RdfFactory):
    def __init__(self, props: Dict[str, str], base_iri: str = None):
        super().__init__(props)
        hostname = "localhost"
        self.base_iri = base_iri or f"http://{hostname}/rdf/ontology/"
        self.tables: Dict[str, Dict] = {}
        self.foreign_keys: List[Dict] = []
        self.initialize()

    def initialize(self) -> None:
        self.initialize_rdf_store()
        self.context = URIRef(self.base_iri)
        add_ontology_to_graph(self.graph)
        self.graph.bind("db", self.base_iri)

    def load_ontology(self, file_path: str) -> None:
        """Load an existing ontology file and rebuild internal mappings."""
        self.initialize_rdf_store()
        self.graph.parse(file_path)
        self.context = URIRef(self.base_iri)
        self.tables = {}
        self.foreign_keys = []

        for table_class in self.graph.subjects(RDFS.subClassOf, DATABASETABLE):
            table_name_literal = self.graph.value(table_class, TABLE)
            if table_name_literal is None:
                continue
            table_name = str(table_name_literal)
            schema_name = self.graph.value(table_class, SCHEMA)
            catalog_name = self.graph.value(table_class, CATALOG)
            columns = []
            primary_keys = []
            for col_class in self.graph.subjects(TABLE, table_class):
                col_name_literal = self.graph.value(col_class, COLUMN)
                if col_name_literal is None:
                    continue
                col_name = str(col_name_literal)
                columns.append(col_name)
                if (col_class, RDFS.subClassOf, PRIMARYKEY) in self.graph:
                    primary_keys.append(col_name)

            self.tables[table_name] = {
                "class": table_class,
                "columns": columns,
                "primary_keys": primary_keys,
                "schema": str(schema_name) if schema_name else None,
                "catalog": str(catalog_name) if catalog_name else None,
            }

        for pred in self.graph.subjects(RDFS.subPropertyOf, COLUMNREFERENCE):
            source = self.graph.value(pred, RDFS.domain)
            target = self.graph.value(pred, RDFS.range)
            if source and target:
                self.foreign_keys.append(
                    {
                        "predicate": pred,
                        "source_class": source,
                        "target_class": target,
                    }
                )

    def get_class_for_table(self, table_name: str) -> URIRef:
        return URIRef(self.base_iri + table_name.replace(" ", "_"))

    def get_class_for_column(self, table_name: str, column_name: str) -> URIRef:
        table_name_uri = table_name.replace(" ", "_")
        column_name_uri = column_name.replace(" ", "_")
        return URIRef(self.base_iri + f"{table_name_uri}.{column_name_uri}")

    def process_table(
        self,
        table_name: str,
        columns: List[str],
        primary_keys: List[str],
        foreign_keys: List[ForeignKeySpecification],
        schema_name: str,
        catalog_name: str,
    ) -> None:
        table_class = self.get_class_for_table(table_name)
        self.graph.add((table_class, RDF.type, OWL.Class))
        self.graph.add((table_class, RDFS.subClassOf, DATABASETABLE))
        self.graph.add((table_class, RDFS.label, Literal(table_name)))
        self.graph.add((table_class, TABLE, Literal(table_name)))
        if catalog_name:
            self.graph.add((table_class, CATALOG, Literal(catalog_name)))
        if schema_name:
            self.graph.add((table_class, SCHEMA, Literal(schema_name)))

        col_list = []
        for col in columns:
            col_class = self.add_column(table_name, col)
            col_list.append(col_class)

        self.add_primary_keys(table_name, primary_keys)
        self.add_foreign_keys(foreign_keys)

        self.tables[table_name] = {
            "class": table_class,
            "columns": columns,
            "primary_keys": primary_keys,
            "schema": schema_name,
            "catalog": catalog_name,
        }

    def add_column(self, table_name: str, column: str) -> URIRef:
        column_class = self.get_class_for_column(table_name, column)
        self.graph.add((column_class, RDF.type, OWL.Class))
        self.graph.add((column_class, RDFS.subClassOf, DATABASECOLUMN))
        self.graph.add((column_class, RDFS.label, Literal(f"{table_name}.{column}")))
        self.graph.add((column_class, TABLE, self.get_class_for_table(table_name)))
        self.graph.add((column_class, COLUMN, Literal(column)))
        return column_class

    def add_primary_keys(self, table_name: str, primary_keys: List[str]) -> None:
        for pk in primary_keys:
            column_class = self.get_class_for_column(table_name, pk)
            self.graph.add((column_class, RDFS.subClassOf, PRIMARYKEY))

    def add_foreign_keys(self, foreign_keys: List[ForeignKeySpecification]) -> None:
        for fk in foreign_keys:
            column_class = self.get_class_for_column(fk.foreign_key_table, fk.foreign_key_column)
            self.graph.add((column_class, RDFS.subClassOf, FOREIGNKEY))

            source_iri = self.add_column(fk.foreign_key_table, fk.foreign_key_column)
            target_iri = self.add_column(fk.primary_key_table, fk.primary_key_column)

            self.foreign_keys.append({
                "source_class": source_iri,
                "target_class": target_iri,
            })

            self.graph.add((source_iri, DBO.has_target_column, target_iri))

    def export_data(self, file_path: str) -> None:
        self.graph.serialize(destination=file_path, format="xml")
