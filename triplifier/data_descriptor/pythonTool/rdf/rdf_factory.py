from rdflib import ConjunctiveGraph, URIRef
from typing import Iterable


class RdfFactory:
    def __init__(self, props: dict):
        self.props = props
        self.graph = ConjunctiveGraph()
        self.context = None

    def initialize_rdf_store(self) -> None:
        repo_type = self.props.get("repo.type", "memory")
        # Only memory store is supported in this python port
        self.graph = ConjunctiveGraph()

    def add_statement(self, subject: URIRef, predicate: URIRef, obj) -> None:
        if self.context:
            self.graph.get_context(self.context).add((subject, predicate, obj))
        else:
            self.graph.add((subject, predicate, obj))

    def add_statements(self, statements: Iterable[tuple]) -> None:
        for s in statements:
            if self.context:
                self.graph.get_context(self.context).add(s)
            else:
                self.graph.add(s)

    def clear_data(self) -> None:
        if self.context:
            self.graph.remove_context(self.context)
        else:
            self.graph.remove((None, None, None))

    def get_all_statements_in_context(self):
        if self.context:
            return list(self.graph.get_context(self.context).triples((None, None, None)))
        return list(self.graph.triples((None, None, None)))
