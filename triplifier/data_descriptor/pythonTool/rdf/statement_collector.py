from typing import List
from rdflib import URIRef, BNode


class StatementCollector:
    def __init__(self, context: URIRef = None):
        self.context = context
        self.statements: List[tuple] = []

    def add_statement(self, subject, predicate, obj) -> None:
        self.statements.append((subject, predicate, obj))

    def get_statements(self):
        return list(self.statements)

    def clear(self):
        self.statements.clear()
