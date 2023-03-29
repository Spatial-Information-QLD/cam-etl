import itertools

from rdflib import Graph, URIRef
from rdflib.namespace import RDF, GEO
from pyspark.sql import SparkSession

from etl_lib.tables import Table
from etl_lib.tables.lf_sp_survey_point import SPSurveyPointTable
from etl_lib.graph import ADDR


class GeocodeTable(Table):
    table = "lalfdb.lalfpdba_lf_geocode"

    GEOCODE_ID = "geocode_id"
    SPDB_PID = "spdb_pid"

    def __init__(self, spark: SparkSession, limit: int = None) -> None:
        super().__init__(spark, limit)

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
