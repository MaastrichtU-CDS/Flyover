from typing import Dict, List, Any
from rdflib import URIRef, Literal, RDF

from .rdf_factory import RdfFactory
from .statement_collector import StatementCollector
from .ontology.dbo import DBO, HAS_COLUMN, HAS_CELL, HAS_VALUE
from .ontology_factory import OntologyFactory
from sqlalchemy import create_engine, text


class DataFactory(RdfFactory):
    def __init__(self, ontology_factory: OntologyFactory, props: Dict[str, str]):
        super().__init__(props)
        hostname = "localhost"
        self.base_iri = props.get("repo.dataUri", f"http://{hostname}/rdf/data/")
        self.ontology_factory = ontology_factory
        self.value_index: Dict[str, Dict[str, List[URIRef]]] = {}
        url = props["db"]["url"]
        if not url:
            raise ValueError("db.url must be provided")
        self.engine = create_engine(url)
        self.initialize()

    def initialize(self) -> None:
        self.initialize_rdf_store()
        self.context = URIRef(self.base_iri)
        self.graph.bind("data", self.base_iri)

    def convert_data(self) -> None:
        with self.engine.connect() as conn:
            for table_name, table_info in self.ontology_factory.tables.items():
                table_class_uri = table_info["class"]
                name = str(table_class_uri).split("/")[-1]
                self.process_table(conn, table_class_uri, name)
            self.generate_foreign_key_relations()

    def process_table(self, conn, table_class_uri: URIRef, table_name: str) -> None:
        result = conn.execute(text(f'SELECT * FROM "{table_name}"'))
        columns = list(result.keys())
        for row_id, row in enumerate(result):
            row_iri = self.get_table_row_iri(table_name, table_class_uri, row_id, columns, row)
            collector = StatementCollector(self.context)
            collector.add_statement(row_iri, RDF.type, table_class_uri)
            self.process_columns(row, columns, table_class_uri, row_iri, collector)
            self.add_statements(collector.get_statements())
            collector.clear()

    def get_table_row_iri(
        self,
        table_name: str,
        table_class_uri: URIRef,
        row_id: int,
        columns: List[str],
        row: Any,
    ) -> URIRef:
        base_iri_table = self.base_iri + table_name + "/"
        iri = URIRef(base_iri_table + str(row_id))

        pk_columns = self.ontology_factory.tables[table_name]["primary_keys"]
        if pk_columns:
            # Ensure compatibility with both dict-like and tuple-like row objects
            try:
                pkey_value = "_".join(str(row[pk]) for pk in pk_columns)
            except (KeyError, TypeError, AttributeError):
                # Fallback for tuple-like rows
                pkey_value = "_".join(str(getattr(row, pk)) for pk in pk_columns)
            iri = URIRef(base_iri_table + pkey_value)
        return iri

    def process_columns(
        self,
        row: Any,
        columns: List[str],
        table_class_uri: URIRef,
        row_iri: URIRef,
        collector: StatementCollector,
    ) -> None:
        table_name = str(table_class_uri).split("/")[-1]
        for col_name in columns:
            try:
                value = row[col_name]
            except (KeyError, TypeError, AttributeError):
                value = getattr(row, col_name)
            column_class_uri = self.ontology_factory.get_class_for_column(
                table_name, col_name
            )
            column_row_iri = URIRef(f"{row_iri}/{col_name}")
            collector.add_statement(column_row_iri, RDF.type, column_class_uri)
            collector.add_statement(row_iri, HAS_COLUMN, column_row_iri)
            if value is not None:
                value_node = URIRef(f"{column_row_iri}/value")
                collector.add_statement(column_row_iri, HAS_CELL, value_node)
                collector.add_statement(value_node, RDF.type, DBO.Cell)
                collector.add_statement(value_node, HAS_VALUE, Literal(str(value)))
                idx = self.value_index.setdefault(str(column_class_uri), {})
                idx.setdefault(str(value), []).append(row_iri)

    def generate_foreign_key_relations(self) -> None:
        for fk in self.ontology_factory.foreign_keys:
            source_idx = self.value_index.get(str(fk["source_class"]), {})
            target_idx = self.value_index.get(str(fk["target_class"]), {})
            for value, subjects in source_idx.items():
                targets = target_idx.get(value, [])
                for s in subjects:
                    for t in targets:
                        self.add_statement(s, DBO.fk_refers_to, t)

    def export_data(self, file_path: str) -> None:
        self.graph.serialize(destination=file_path, format="nt")
