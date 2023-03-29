import itertools

from rdflib import Graph, URIRef, Literal
from rdflib.namespace import GEO
from pyspark.sql import SparkSession

from etl_lib.tables import Table


class SPSurveyPointTable(Table):
    table = "lalfdb.lalfpdba_sp_survey_point"

    PID = "pid"
    CENTROID_LON = "centroid_lon"
    CENTROID_LAT = "centroid_lat"

    def __init__(self, spark: SparkSession, limit: int = None) -> None:
        super().__init__(spark, limit)

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
                        f"POINT ({row[SPSurveyPointTable.CENTROID_LON]} {row[SPSurveyPointTable.CENTROID_LAT]})"
                    ),
                )
            )

        Table.to_file(table_name, graph)
