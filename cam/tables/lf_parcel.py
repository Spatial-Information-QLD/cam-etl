import itertools
from pathlib import Path

from rdflib import URIRef
from rdflib.namespace import RDF
from pyspark.sql import SparkSession
from jinja2 import Template

from cam.tables import Table
from cam.graph import ADDR, create_graph


class ParcelTable(Table):
    table = "lalfdb.lalfpdba_lf_parcel"

    PARCEL_ID = "parcel_id"

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
                    select p.parcel_id
                    from lalfdb.lalfpdba_lf_parcel p
                    join lalfdb.lalfpdba_lf_site s on s.parcel_id = p.parcel_id
                    {% if site_ids %}
                    where
                        s.site_id in {{ site_ids }}
                    {% endif %}
                ) as parcel
            """
                ).render(site_ids=site_ids),
            )
            .load()
        )

    @staticmethod
    def get_iri(parcel_id: str):
        return URIRef(f"https://linked.data.gov.au/dataset/qld-cad/{parcel_id}")

    @staticmethod
    def transform(rows: itertools.chain, table_name: str):
        oxigraph_path = Path(f"oxigraph_data/{table_name}")
        graph = create_graph(str(oxigraph_path))

        for row in rows:
            parcel_id = row[ParcelTable.PARCEL_ID]
            iri = ParcelTable.get_iri(parcel_id)
            graph.add((iri, RDF.type, ADDR.AddressableObject))

        Table.to_file(table_name, graph)
        graph.close()
