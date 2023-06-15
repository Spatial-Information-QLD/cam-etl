import itertools
from pathlib import Path

from rdflib import URIRef, Literal, BNode
from rdflib.namespace import RDF, SDO
from pyspark.sql import SparkSession
from jinja2 import Template

from cam.tables import Table
from cam.graph import ROADS, RCT, CN, create_graph


class QRTRoadsTable(Table):
    table = "lalfdb.qrt"

    ROAD_ID = "road_id"
    ROAD_PREFIX = "road_prefix"
    ROAD_NAME = "road_name"
    ROAD_TYPE = "road_type"
    ROAD_SUFFIX = "road_suffix"

    def __init__(self, spark: SparkSession, site_ids: str = None) -> None:
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
                        select distinct
                            s.site_id,
                            q.road_id,
                            null as "road_prefix",
                            q.road_name,
                            q.road_type,
                            NULLIF(q.road_suffix, '') as "road_suffix"
                        from lalfdb.lalfpdba_lf_address a 
                            join lalfdb.lalfpdba_lf_road r on r.road_id = a.road_id
                            join lalfdb.lalfpdba_locality l on l.locality_code = r.locality_code
                            join lalfdb.qrt q on q.road_name_basic = r.qrt_road_name_basic and q.address_locality = l.locality_name
                            join lalfdb.lalfpdba_lf_site s on s.site_id = a.site_id
                            join lalfdb.lalfpdba_lf_geocode g on g.site_id = s.site_id
                            join lalfdb.lalfpdba_sp_survey_point sp on sp.pid = g.spdb_pid
                        where
                            q.record_status = 'Current'
                            and a.addr_status_code != 'H'
                            {% if site_ids %}
                            and s.site_id IN {{ site_ids }}
                            {% endif %}
                    ) AS qrt
                """
                ).render(site_ids=site_ids),
            )
            .load()
        )

    @staticmethod
    def get_iri(road_id: str):
        return URIRef(f"https://linked.data.gov.au/dataset/qld-addr/road-{road_id}")

    @staticmethod
    def get_label_iri(road_id: str):
        return URIRef(
            f"https://linked.data.gov.au/dataset/qld-addr/road-label-{road_id}"
        )

    @staticmethod
    def transform(rows: itertools.chain, table_name: str):
        oxigraph_path = Path(f"oxigraph_data/{table_name}")
        graph = create_graph(str(oxigraph_path))

        for row in rows:
            iri = QRTRoadsTable.get_iri(row[QRTRoadsTable.ROAD_ID])
            label_iri = QRTRoadsTable.get_label_iri(row[QRTRoadsTable.ROAD_ID])

            graph.add((iri, RDF.type, ROADS.RoadObject))
            graph.add((label_iri, RDF.type, ROADS.RoadLabel))
            graph.add((label_iri, RDF.type, CN.CompoundName))

            graph.add((iri, SDO.name, label_iri))
            graph.add((label_iri, CN.isNameFor, iri))

            # Road Prefix
            road_prefix = row[QRTRoadsTable.ROAD_PREFIX]
            if road_prefix is not None:
                bnode = BNode()
                graph.add((label_iri, SDO.hasPart, bnode))
                graph.add(
                    (
                        bnode,
                        SDO.additionalType,
                        RCT.RoadPrefix,
                    )
                )
                graph.add((bnode, RDF.value, Literal(road_prefix)))

            # Road Name
            road_name = row[QRTRoadsTable.ROAD_NAME]
            if road_name is not None:
                bnode = BNode()
                graph.add((label_iri, SDO.hasPart, bnode))
                graph.add(
                    (
                        bnode,
                        SDO.additionalType,
                        RCT.RoadName,
                    )
                )
                graph.add((bnode, RDF.value, Literal(road_name)))

            # Road Type
            road_type = row[QRTRoadsTable.ROAD_TYPE]
            if road_type is not None:
                bnode = BNode()
                graph.add((label_iri, SDO.hasPart, bnode))
                graph.add(
                    (
                        bnode,
                        SDO.additionalType,
                        RCT.RoadType,
                    )
                )
                graph.add((bnode, RDF.value, Literal(road_type)))

            # Road Suffix
            road_suffix = row[QRTRoadsTable.ROAD_SUFFIX]
            if road_suffix is not None:
                bnode = BNode()
                graph.add((label_iri, SDO.hasPart, bnode))
                graph.add(
                    (
                        bnode,
                        SDO.additionalType,
                        RCT.RoadSuffix,
                    )
                )
                graph.add((bnode, RDF.value, Literal(road_suffix)))

        Table.to_file(table_name, graph)
        graph.close()
