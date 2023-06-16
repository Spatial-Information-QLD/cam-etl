import itertools
from pathlib import Path

from rdflib import URIRef, Literal, BNode
from rdflib.namespace import RDF, RDFS, SDO, DCTERMS, GEO, SKOS, PROV
from pyspark.sql import SparkSession
from jinja2 import Template

from cam.tables import Table
from cam.graph import create_graph, GN, CN, GPT


class GazettedPlaceNmTable(Table):
    table = "lalfdb.lapnpdba_placenm"

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
                        pn.ref_no,
                        pn.placename,
                        pn.placetype,
                        pn.latcoord,
                        pn.loncoord,
                        pn.gaz_date,
                        pnc.comments,
                        pnc.origin,
                        pnc.history
                    from lalfdb.lapnpdba_placenm pn
                    join lalfdb.lapnpdba_pncomment pnc on pnc.ref_no = pn.ref_no
                ) AS placenm
            """
                ).render(),
            )
            .load()
        )

    @staticmethod
    def get_iri(placename_id: str):
        return URIRef(
            f"https://linked.data.gov.au/dataset/qld-addr/pn-o/g/{placename_id}"
        )

    @staticmethod
    def transform(rows: itertools.chain, table_name: str):
        oxigraph_path = Path(f"oxigraph_data/{table_name}")
        graph = create_graph(str(oxigraph_path))

        REF_NO = "ref_no"
        PLACENAME = "placename"
        PLACETYPE = "placetype"
        LATCOORD = "latcoord"
        LONCOORD = "loncoord"
        GAZ_DATE = "gaz_date"
        COMMENTS = "comments"
        ORIGIN = "origin"
        HISTORY = "history"

        for row in rows:
            # Geographical Object
            ref = row[REF_NO]
            iri = GazettedPlaceNmTable.get_iri(ref)
            graph.add((iri, RDF.type, GN.GeographicalObject))

            # lat long
            geometry_iri = BNode()
            lat = row[LATCOORD]
            long = row[LONCOORD]
            if lat is not None and long is not None:
                graph.add((iri, GEO.hasGeometry, geometry_iri))
                graph.add((geometry_iri, RDF.type, GEO.Geometry))
                graph.add(
                    (
                        geometry_iri,
                        GEO.asWKT,
                        Literal(f"POINT ({long} {lat})", datatype=GEO.wktLiteral),
                    )
                )

            # placetype
            placetype: str = row[PLACETYPE]
            if placetype is not None:
                graph.add(
                    (
                        iri,
                        SDO.additionalType,
                        URIRef(
                            f"https://linked.data.gov.au/def/go-categories/{placetype.lower()}"
                        ),
                    )
                )

            # Geographical Name
            geographical_name_iri = URIRef(
                f"https://linked.data.gov.au/dataset/qld-addr/pn/g/{ref}"
            )
            graph.add((geographical_name_iri, CN.isNameFor, iri))
            graph.add((geographical_name_iri, RDF.type, GN.GeographicalName))
            graph.add(
                (geographical_name_iri, SDO.additionalType, GPT.GeographicalGivenName)
            )
            graph.add(
                (
                    geographical_name_iri,
                    RDF.value,
                    Literal(row[PLACENAME]),
                )
            )

            # comment
            comment = row[COMMENTS]
            if comment is not None and comment != "":
                graph.add((geographical_name_iri, RDFS.comment, Literal(comment)))

            # origin
            origin: str = row[ORIGIN]
            if origin is not None and origin != "":
                graph.add((geographical_name_iri, DCTERMS.description, Literal(origin)))

            # history
            history = row[HISTORY]
            if history is not None and origin != "":
                graph.add((geographical_name_iri, SKOS.historyNote, Literal(history)))

            # authority
            attribution_iri = BNode()
            graph.add(
                (geographical_name_iri, PROV.qualifiedAttribution, attribution_iri)
            )

            # agent
            agent_iri = BNode()
            graph.add((attribution_iri, PROV.agent, agent_iri))
            graph.add((agent_iri, SDO.name, Literal("Queensland Placenames Authority")))

            # role
            graph.add(
                (
                    attribution_iri,
                    PROV.hadRole,
                    URIRef(
                        "https://linked.data.gov.au/def/iso19115-1/RoleCode/custodian"
                    ),
                )
            )

        Table.to_file(table_name, graph)
        graph.close()
