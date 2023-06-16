import itertools
from pathlib import Path

from rdflib import URIRef, Literal
from rdflib.namespace import RDF, SKOS, SDO
from pyspark.sql import SparkSession
from jinja2 import Template

from cam.tables import Table
from cam.graph import create_graph, ADDRCMPType


class LevelTypeTable(Table):
    """
    We currently generate the the level type codelist instead of
    re-using ICSM's one generated from the G-NAF.

    See https://github.com/GeoscienceAustralia/icsm-vocabs/pull/13/files#diff-e463f4b6f11fe1716b181054b563c5f58891534795537ef11c0f2e98b6d1812c
    """

    table = "lalfdb.lalfpdba_lf_level_type"

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
                        l.level_type_code,
                        l.level_type,
                        l.level_type_desc
                    from lalfdb.lalfpdba_lf_level_type l
                ) as level_type
            """
                ).render(),
            )
            .load()
        )

    @staticmethod
    def get_iri(level_type_code: str):
        return URIRef(
            f"https://linked.data.gov.au/def/qld-level-type/{level_type_code}"
        )

    @staticmethod
    def transform(rows: itertools.chain, table_name: str):
        oxigraph_path = Path(f"oxigraph_data/{table_name}")
        graph = create_graph(str(oxigraph_path))

        LEVEL_TYPE_CODE = "level_type_code"
        LEVEL_TYPE = "level_type"
        LEVEL_TYPE_DESC = "level_type_desc"

        for row in rows:
            iri = LevelTypeTable.get_iri(row[LEVEL_TYPE_CODE])
            graph.add((iri, RDF.type, SKOS.Concept))
            graph.add((iri, SKOS.prefLabel, Literal(row[LEVEL_TYPE])))
            graph.add((iri, SKOS.notation, Literal(row[LEVEL_TYPE_CODE])))

            graph.add((iri, RDF.value, Literal(row[LEVEL_TYPE])))
            graph.add((iri, SDO.additionalType, ADDRCMPType.levelTypeCode))

            description: str = row[LEVEL_TYPE_DESC]
            if description is not None and description.strip() != "":
                graph.add((iri, SKOS.definition, Literal(description)))

        Table.to_file(table_name, graph)
        graph.close()
