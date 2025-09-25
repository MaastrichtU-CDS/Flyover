from typing import List, Dict
from sqlalchemy import create_engine, inspect
from .foreign_key_specification import ForeignKeySpecification


class DatabaseInspector:
    """Database inspector using SQLAlchemy."""

    def __init__(self, props: Dict[str, str]):
        self.props = props
        url = props['db']['url']
        if not url:
            raise ValueError("db.url must be provided")
        self.engine = create_engine(url)
        self.inspector = inspect(self.engine)

    def get_table_names(self) -> List[Dict[str, str]]:
        return [
            {"name": name, "catalog": None, "schema": None}
            for name in self.inspector.get_table_names()
        ]

    def get_column_names(self, table_name: str) -> List[str]:
        return [c["name"] for c in self.inspector.get_columns(table_name)]

    def get_primary_key_columns(self, catalog: str, schema: str, table_name: str) -> List[str]:
        pk = self.inspector.get_pk_constraint(table_name)
        return pk.get("constrained_columns", [])

    def get_foreign_key_columns(self, catalog: str, schema: str, table_name: str) -> List[ForeignKeySpecification]:
        fks = []
        for fk in self.inspector.get_foreign_keys(table_name):
            for local, remote in zip(
                fk.get("constrained_columns", []),
                fk.get("referred_columns", []),
            ):
                fks.append(
                    ForeignKeySpecification(
                        primary_key_table=fk.get("referred_table"),
                        primary_key_column=remote,
                        foreign_key_table=table_name,
                        foreign_key_column=local,
                    )
                )
        return fks
