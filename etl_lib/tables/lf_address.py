import itertools

from rdflib import Graph, URIRef
from rdflib.namespace import RDF
from pyspark.sql import SparkSession

from etl_lib.tables import Table
from etl_lib.tables.lf_site import SiteTable
from etl_lib.tables.lf_geocode import GeocodeTable
from etl_lib.graph import ADDR


class AddressTable(Table):
    table = "lalfdb.lalfpdba_lf_address"

    ADDR_ID = "addr_id"

    def __init__(self, spark: SparkSession, limit: int = None) -> None:
        super().__init__(spark, limit)

    @staticmethod
    def get_iri(addr_id: str):
        return URIRef(f"https://linked.data.gov.au/dataset/qld-addr/addr-{addr_id}")

    @staticmethod
    def transform(rows: itertools.chain, graph: Graph, table_name: str):
        for row in rows:
            iri = AddressTable.get_iri(row[AddressTable.ADDR_ID])
            graph.add((iri, RDF.type, ADDR.Address))

            site_iri = SiteTable.get_iri(row[SiteTable.SITE_ID])
            graph.add((iri, ADDR.isAddressFor, site_iri))

            geocode_iri = GeocodeTable.get_iri(row[GeocodeTable.GEOCODE_ID])
            graph.add((iri, ADDR.hasGeocode, geocode_iri))

        Table.to_file(table_name, graph)
