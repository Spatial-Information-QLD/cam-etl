import itertools

from rdflib import Graph, URIRef
from rdflib.namespace import RDF, GEO
from pyspark.sql import SparkSession
from jinja2 import Template

from cam.tables import Table
from cam.tables.lf_sp_survey_point import SPSurveyPointTable
from cam.graph import ADDR


class GeocodeTable(Table):
    table = "lalfdb.lalfpdba_lf_geocode"

    GEOCODE_ID = "geocode_id"
    SPDB_PID = "spdb_pid"

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
                    select g.*
                    from lalfdb.lalfpdba_lf_geocode g
                        join lalfdb.lalfpdba_lf_site s on s.site_id = g.site_id
                    {% if site_ids %}
                    where
                        s.site_id in {{ site_ids }}
                    {% endif %}
                ) AS geocode
            """
                ).render(site_ids=site_ids),
            )
            .load()
        )

    @staticmethod
    def get_iri(geocode_id: str):
        return URIRef(
            f"https://linked.data.gov.au/dataset/qld-addr/geocode-{geocode_id}"
        )

    @staticmethod
    def transform(rows: itertools.chain, graph: Graph, table_name: str):
        for row in rows:
            iri = GeocodeTable.get_iri(row[GeocodeTable.GEOCODE_ID])
            graph.add((iri, RDF.type, ADDR.Geocode))

            point_iri = SPSurveyPointTable.get_iri(row[GeocodeTable.SPDB_PID])
            graph.add((iri, GEO.hasGeometry, point_iri))

        Table.to_file(table_name, graph)
