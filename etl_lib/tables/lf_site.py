import itertools

from rdflib import Graph, URIRef
from rdflib.namespace import RDF
from pyspark.sql import SparkSession

from etl_lib.tables import Table
from etl_lib.graph import ADDR


class SiteTable(Table):
    table = "lalfdb.lalfpdba_lf_site"

    SITE_ID = "site_id"
    PARENT_SITE_ID = "parent_site_id"
    SITE_TYPE_CODE = "site_type_code"
    SITE_STATUS_CODE = "site_status_code"
    PARCEL_ID = "parcel_id"
    VERSION_NO = "version_no"

    def __init__(self, spark: SparkSession, limit: int = None) -> None:
        super().__init__(spark, limit)

    @staticmethod
    def get_iri(site_id: str):
        return URIRef(f"https://linked.data.gov.au/dataset/qld-addr/addr-obj-{site_id}")

    @staticmethod
    def transform(rows: itertools.chain, graph: Graph, table_name: str):
        for row in rows:
            iri = SiteTable.get_iri(row[SiteTable.SITE_ID])
            graph.add((iri, RDF.type, ADDR.AddressableObject))

        Table.to_file(table_name, graph)
