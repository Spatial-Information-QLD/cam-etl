import itertools
from pathlib import Path

from rdflib import URIRef, Literal
from rdflib.namespace import RDF, SKOS, SDO
from pyspark.sql import SparkSession
from jinja2 import Template

from cam.tables import Table
from cam.graph import create_graph, ADDRCMPType


class UnitTypeTable(Table):
    """
    We currently generate the the unit type (flat type) codelist instead of
    re-using ICSM's one generated from the G-NAF.

    See https://github.com/GeoscienceAustralia/icsm-vocabs/pull/13/files#diff-1c55654e1372d2b9f3a6aa15ee73ca7c745f602f2565b90fbf95a3d078b2aa42
    """

    table = "lalfdb.lalfpdba_lf_unit_type"

    def __init__(self, spark: SparkSession, site_ids: str = None) -> None:
        super().__init__(spark)

        self.df = (
            spark.read.format("jdbc")
            .option(
                "url",
                "jdbc:postgresql://localhost:5432/address?user=postgres&password=postgres",
            )
            .option("driver", "org.postgresql.Driver")
            .option(
                "dbtable",
                Template(
                    """
                (
                    select
                        u.unit_type_code,
                        u.unit_type,
                        u.unit_type_desc
                    from lalfdb.lalfpdba_lf_unit_type u
                ) as unit_type
            """
                ).render(),
            )
            .load()
        )

    @staticmethod
    def get_iri(unit_type_code: str):
        return URIRef(f"https://linked.data.gov.au/def/qld-flat-type/{unit_type_code}")

    @staticmethod
    def transform(rows: itertools.chain, table_name: str):
        oxigraph_path = Path(f"oxigraph_data/{table_name}")
        graph = create_graph(str(oxigraph_path))

        UNIT_TYPE_CODE = "unit_type_code"
        UNIT_TYPE = "unit_type"
        UNIT_TYPE_DESC = "unit_type_desc"

        for row in rows:
            iri = UnitTypeTable.get_iri(row[UNIT_TYPE_CODE])
            graph.add((iri, RDF.type, SKOS.Concept))
            graph.add((iri, SKOS.prefLabel, Literal(row[UNIT_TYPE])))
            graph.add((iri, SKOS.notation, Literal(row[UNIT_TYPE_CODE])))
            
            graph.add((iri, RDF.value, Literal(row[UNIT_TYPE])))
            graph.add((iri, SDO.additionalType, ADDRCMPType.flatTypeCode))

            description: str = row[UNIT_TYPE_DESC]
            if description is not None and description.strip() != "":
                graph.add((iri, SKOS.definition, Literal(description)))

        Table.to_file(table_name, graph)
        graph.close()
