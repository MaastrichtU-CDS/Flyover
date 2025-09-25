from dataclasses import dataclass

@dataclass
class ForeignKeySpecification:
    primary_key_table: str
    primary_key_column: str
    foreign_key_table: str
    foreign_key_column: str
