from typing import Dict
from rdflib import URIRef

from .rdf_factory import RdfFactory


class AnnotationFactory(RdfFactory):
    def __init__(self, props: Dict[str, str]):
        super().__init__(props)
        self.initialize()

    def initialize(self) -> None:
        self.initialize_rdf_store()
        self.context = URIRef("http://annotation.local/")
