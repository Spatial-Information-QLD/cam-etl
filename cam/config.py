from pydantic import BaseModel
from pydantic_yaml import YamlModel


class IRI(BaseModel):
    table: str
    field_mapping: dict[str, str]


class LITERAL(BaseModel):
    datatype: str
    lang: str = None


class Instance(BaseModel):
    types: list[str]
    iri_template: str
    field_mapping: list[str]
    filters: dict[str, str]
    blank_node: bool = False


class Column(BaseModel):
    predicate: str
    object: IRI | LITERAL


class Table(BaseModel):
    instance: Instance
    columns: dict[str, Column] = None


class ETL(BaseModel):
    connection: str
    tables: dict[str, Table]


class ETLConfig(YamlModel):
    prefixes: dict[str, str]
    etl: ETL


class Config(YamlModel):
    db_connection: str
    tables: list[str]
    site_ids: str = None
