import itertools
from pathlib import Path

from rdflib import URIRef, Literal
from rdflib.namespace import RDF, SDO, SKOS
from pyspark.sql import SparkSession
from jinja2 import Template

from cam.tables import Table
from cam.tables.lf_parcel import ParcelTable
from cam.graph import create_graph, GN, CN, GPT


class PlacenameTable(Table):
    table = "lalfdb.lalfpdba_lf_place_name"

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
                        p.parcel_id,
                        pn.pl_name_id,
                        pn.pl_name,
                        pnds.pl_name_data_source
                    from lalfdb.lalfpdba_lf_place_name pn
                    join lalfdb.lalfpdba_lf_place_name_data_source pnds on pnds.pl_name_data_source_code = pn.pl_name_data_source_code
                    join lalfdb.lalfpdba_lf_site s on s.site_id = pn.site_id
                    join lalfdb.lalfpdba_lf_parcel p on p.parcel_id = s.parcel_id
                    where pn.pl_name_status_code != 'H'
                    {% if site_ids %}and pn.site_id in {{ site_ids }}{% endif %}
                ) AS placenm
            """
                ).render(site_ids=site_ids),
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

        PARCEL_ID = "parcel_id"
        NAME_ID = "pl_name_id"
        NAME = "pl_name"
        NAME_DATA_SOURCE = "pl_name_data_source"

        for row in rows:
            iri = PlacenameTable.get_iri(row[NAME_ID])
            graph.add((iri, RDF.type, GN.GeographicalName))
            graph.add((iri, SDO.additionalType, GPT.GeographicalGivenName))

            # is name for
            parcel_iri = ParcelTable.get_iri(row[PARCEL_ID])
            graph.add((iri, CN.isNameFor, parcel_iri))

            # value
            graph.add((iri, RDF.value, Literal(row[NAME])))

            # history
            history_note = row[NAME_DATA_SOURCE]
            if history_note is not None:
                graph.add((iri, SKOS.historyNote, Literal(history_note)))

        Table.to_file(table_name, graph)
        graph.close()
