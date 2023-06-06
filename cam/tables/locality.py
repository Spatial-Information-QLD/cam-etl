import itertools
from pathlib import Path

from rdflib import URIRef, Literal
from rdflib.namespace import RDF, RDFS, SDO
from pyspark.sql import SparkSession
from jinja2 import Template

from cam.tables import Table
from cam.graph import ADDR, ADDRCMPType, create_graph


class LocalityTable(Table):
    table = "lalfdb.lalfpdba_locality"

    LOCALITY_NAME = "locality_name"

    def __init__(self, spark: SparkSession, site_ids: list[str] = None) -> None:
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
                    select distinct l.locality_name
                    from lalfdb.lalfpdba_locality l
                ) AS locality
            """
                ).render(),
            )
            .load()
        )

    @staticmethod
    def get_iri(locality_name: str):
        return URIRef(
            f"https://linked.data.gov.au/dataset/qld-addr/locality-{locality_name.replace(' ', '-')}"
        )

    @staticmethod
    def transform(rows: itertools.chain, table_name: str):
        oxigraph_path = Path(f"oxigraph_data/{table_name}")
        graph = create_graph(str(oxigraph_path))

        for row in rows:
            iri = LocalityTable.get_iri(row[LocalityTable.LOCALITY_NAME])

            # TODO: check class type in model.
            graph.add((iri, RDF.type, ADDR.Locality))
            graph.add((iri, SDO.additionalType, ADDRCMPType.locality))
            graph.add((iri, RDFS.label, Literal(row[LocalityTable.LOCALITY_NAME])))
            graph.add((iri, RDF.value, Literal(row[LocalityTable.LOCALITY_NAME])))

        Table.to_file(table_name, graph)
        graph.close()
