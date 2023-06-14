import itertools
from pathlib import Path

from rdflib import URIRef, Literal
from rdflib.namespace import RDF, SDO, SKOS
from pyspark.sql import SparkSession
from jinja2 import Template

from cam.tables import Table
from cam.tables.lf_site import SiteTable
from cam.graph import create_graph, GN, CN, GPT


class PlacenameTable(Table):
    table = "lalfdb.lalfpdba_lf_place_name"

    NAME_ID = "pl_name_id"
    NAME = "pl_name"
    SITE_ID = "site_id"
    NAME_DATA_SOURCE = "pl_name_data_source"

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
                    select
                        pn.pl_name_id,
                        pn.pl_name,
                        pn.site_id,
                        pnds.pl_name_data_source
                    from lalfdb.lalfpdba_lf_place_name pn
                    join lalfdb.lalfpdba_lf_place_name_data_source pnds on pnds.pl_name_data_source_code = pn.pl_name_data_source_code
                    where pn.pl_name_status_code != 'H'
                ) AS placenm
            """
                ).render(),
            )
            .load()
        )

    @staticmethod
    def get_iri(placename_id: str):
        return URIRef(
            f"https://linked.data.gov.au/dataset/qld-addr/pn-o/{placename_id}"
        )

    @staticmethod
    def transform(rows: itertools.chain, table_name: str):
        oxigraph_path = Path(f"oxigraph_data/{table_name}")
        graph = create_graph(str(oxigraph_path))

        for row in rows:
            iri = PlacenameTable.get_iri(row[PlacenameTable.NAME_ID])
            graph.add((iri, RDF.type, GN.GeographicalName))
            graph.add((iri, SDO.additionalType, GPT.GeographicalGivenName))

            # is name for
            site_iri = SiteTable.get_iri(row[PlacenameTable.SITE_ID])
            graph.add((iri, CN.isNameFor, site_iri))

            # value
            graph.add((iri, RDF.value, Literal(row[PlacenameTable.NAME])))

            # history
            history_note = row[PlacenameTable.NAME_DATA_SOURCE]
            if history_note is not None:
                graph.add((iri, SKOS.historyNote, Literal(history_note)))

        Table.to_file(table_name, graph)
        graph.close()
