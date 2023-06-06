import itertools

from rdflib import Graph, URIRef, Literal
from rdflib.namespace import GEO
from pyspark.sql import SparkSession
from jinja2 import Template

from cam.tables import Table


class SPSurveyPointTable(Table):
    table = "lalfdb.lalfpdba_sp_survey_point"

    PID = "pid"
    CENTROID_LON = "centroid_lon"
    CENTROID_LAT = "centroid_lat"

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
                    select sp.*
                    from lalfdb.lalfpdba_sp_survey_point sp
                        join lalfdb.lalfpdba_lf_geocode g on g.spdb_pid = sp.pid
                        join lalfdb.lalfpdba_lf_site s on s.site_id = g.site_id
                    {% if site_ids %}
                    where
                        s.site_id in {{ site_ids }}
                    {% endif %}
                ) AS survey_point
            """
                ).render(site_ids=site_ids),
            )
            .load()
        )

    @staticmethod
    def get_iri(pid: str):
        return URIRef(
            f"https://linked.data.gov.au/dataset/qld-addr/sp-survey-point-{pid}"
        )

    @staticmethod
    def transform(rows: itertools.chain, graph: Graph, table_name: str):
        for row in rows:
            iri = SPSurveyPointTable.get_iri(row[SPSurveyPointTable.PID])
            graph.add(
                (
                    iri,
                    GEO.asWKT,
                    Literal(
                        f"POINT ({row[SPSurveyPointTable.CENTROID_LON]} {row[SPSurveyPointTable.CENTROID_LAT]})",
                        datatype=GEO.wktLiteral,
                    ),
                )
            )

        Table.to_file(table_name, graph)
