import argparse
import yaml
import logging

from .db_inspector import DatabaseInspector
from .rdf.ontology_factory import OntologyFactory
from .rdf.data_factory import DataFactory
from .rdf.annotation_factory import AnnotationFactory


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("triplifier")


def run_triplifier(args: argparse.Namespace) -> None:
    with open(args.config) as f:
        props = yaml.safe_load(f) or {}
    
    of = OntologyFactory(props, base_iri=args.baseuri)
    db_inspect = DatabaseInspector(props)

    mode = args.ontologyAndOrData

    if mode != "data":
        logger.info("Extracting ontology")
        for table in db_inspect.get_table_names():
            columns = db_inspect.get_column_names(table["name"])
            pks = db_inspect.get_primary_key_columns(
                table["catalog"], table["schema"], table["name"]
            )
            fks = db_inspect.get_foreign_key_columns(
                table["catalog"], table["schema"], table["name"]
            )
            of.process_table(
                table["name"], columns, pks, fks, table["schema"], table["catalog"]
            )
        of.export_data(args.ontology)
    else:
        logger.info("Loading ontology from file")
        of.load_ontology(args.ontology)

    if mode != "ontology":
        df = DataFactory(of, props)
        logger.info("Extracting data")
        df.convert_data()
        df.export_data(args.output)


def main():
    #get hostname
    import socket
    hostname = socket.gethostname()

    parser = argparse.ArgumentParser(description="Python Triplifier")
    parser.add_argument("-c", "--config", default="triplifier.yaml")
    parser.add_argument("-o", "--output", default="output.ttl")
    parser.add_argument("-t", "--ontology", default="ontology.owl")
    parser.add_argument("-b", "--baseuri", default=f"http://{hostname}/")
    parser.add_argument(
        "--ontologyAndOrData",
        choices=["ontology", "data"],
        default=None,
        help="Only convert ontology or data; default converts both",
    )
    args = parser.parse_args()
    run_triplifier(args)


if __name__ == "__main__":
    main()
