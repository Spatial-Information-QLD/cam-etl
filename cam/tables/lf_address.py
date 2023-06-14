import itertools
from pathlib import Path

from rdflib import Graph, URIRef, BNode, Literal
from rdflib.namespace import RDF, SDO, SKOS
from pyspark.sql import SparkSession
from jinja2 import Template

from cam.tables import Table
from cam.tables.lf_site import SiteTable
from cam.tables.lf_geocode import GeocodeTable
from cam.tables.qrt import QRTRoadsTable
from cam.tables.locality import LocalityTable
from cam.graph import ADDR, ADDRCMPType, ACTISO, CN, create_graph
from cam.remote_concepts import get_remote_concepts


FSDF_SPARQL_ENDPOINT = "https://gnaf.linked.fsdf.org.au/sparql"

FLAT_TYPE_CODE_IRI = "https://linked.data.gov.au/dataset/gnaf/code/flatType"
LEVEL_TYPE_CODE_IRI = "https://linked.data.gov.au/dataset/gnaf/code/levelType"


def add_addr_component(
    column_value: str,
    component_type: URIRef,
    component_value: URIRef | Literal,
    iri: URIRef,
    graph: Graph,
) -> None:
    """Add an address component to an address object.

    :param column_value: The column value - used in the if guard to check whether this function should run or not.
    :param component_type: The component type IRI.
    :param component_value: The component value. Can either be an IRI or a literal.
    :param iri: The feature, in this case, the address.
    :param graph: The RDF graph where the statements will be added.
    """
    if column_value is not None and column_value.strip() != "":
        component_iri = BNode()
        graph.add((iri, SDO.hasPart, component_iri))
        graph.add((component_iri, SDO.additionalType, component_type))
        graph.add((component_iri, RDF.value, component_value))


class AddressTable(Table):
    table = "lalfdb.lalfpdba_lf_address"

    ADDR_ID = "addr_id"
    UNIT_TYPE_CODE = "unit_type_code"
    UNIT_NO = "unit_no"
    UNIT_SUFFIX = "unit_suffix"
    LEVEL_TYPE_CODE = "level_type_code"
    LEVEL_NO = "level_no"
    STREET_NO_FIRST = "street_no_first"
    STREET_NO_FIRST_SUFFIX = "street_no_first_suffix"
    STREET_NO_LAST = "street_no_last"
    STREET_NO_LAST_SUFFIX = "street_no_last_suffix"
    QRT_ROAD_ID = "qrt_road_id"
    LOCALITY_NAME = "locality_name"
    STATE = "state"
    POSTCODE = "postcode"

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
                    select distinct 
                        a.*,
                        q.road_id as qrt_road_id,
                        l.locality_name,
                        l.state,
                        p.postcode
                    from lalfdb.lalfpdba_lf_site s
                        join lalfdb.lalfpdba_lf_address a on a.site_id = s.site_id 
                        join lalfdb.lalfpdba_lf_geocode g on g.site_id = s.site_id
                        join lalfdb.lalfpdba_sp_survey_point sp on sp.pid = g.spdb_pid
                        join lalfdb.qld_postcode_polygon pp on lalfdb.st_intersects(pp.wkt, lalfdb.st_geomfromtext(sp.wkt_literal, 4326))
                        join lalfdb.postcode p on p.pc_pid = pp.pc_pid
                        join lalfdb.lalfpdba_lf_road r on r.road_id = a.road_id 
                        join lalfdb.lalfpdba_locality l on l.locality_code = r.locality_code 
                        join lalfdb.qrt q on q.road_name_basic = r.qrt_road_name_basic and q.address_locality = l.locality_name 
                    where 
                        q.record_status = 'Current'
                        and a.addr_status_code != 'H'
                        {% if site_ids %}and s.site_id IN {{ site_ids }}{% endif %}
                ) AS addr
            """
                ).render(site_ids=site_ids),
            )
            .load()
        )

    @staticmethod
    def get_iri(addr_id: str):
        return URIRef(f"https://linked.data.gov.au/dataset/qld-addr/addr-{addr_id}")

    @staticmethod
    def get_state_iri(state: str):
        return URIRef(f"https://linked.data.gov.au/dataset/qld-addr/state-{state}")

    @staticmethod
    def transform(rows: itertools.chain, table_name: str):
        oxigraph_path = Path(f"oxigraph_data/{table_name}")
        graph = create_graph(str(oxigraph_path))

        # Fetch FSDF vocabularies
        flat_type_codes = get_remote_concepts(
            FSDF_SPARQL_ENDPOINT,
            FLAT_TYPE_CODE_IRI,
        )
        level_type_codes = get_remote_concepts(
            FSDF_SPARQL_ENDPOINT, LEVEL_TYPE_CODE_IRI
        )

        # Process each row in the results
        for row in rows:
            # addr_id
            iri = AddressTable.get_iri(row[AddressTable.ADDR_ID])
            graph.add((iri, RDF.type, ADDR.Address))
            graph.add((iri, RDF.type, CN.CompoundName))

            site_iri = SiteTable.get_iri(row[SiteTable.SITE_ID])
            graph.add((iri, ADDR.isAddressFor, site_iri))
            graph.add((site_iri, ADDR.hasAddress, iri))
            graph.add((iri, CN.isNameFor, site_iri))
            graph.add((site_iri, SDO.name, iri))

            # Create and add QLD state
            state = row[AddressTable.STATE]
            state_iri = AddressTable.get_state_iri(state)
            graph.add((state_iri, RDF.type, SKOS.Concept))
            graph.add((state_iri, RDF.value, Literal(state)))
            graph.add((state_iri, SDO.additionalType, ADDRCMPType.stateOrTerritory))
            add_addr_component(
                state, ADDRCMPType.stateOrTerritory, state_iri, iri, graph
            )

            # Create and add country
            country = "Australia"
            country_iri = URIRef(
                f"https://linked.data.gov.au/dataset/qld-addr/country-{country}"
            )
            graph.add((country_iri, RDF.type, SKOS.Concept))
            graph.add((country_iri, RDF.value, Literal(country)))
            graph.add((country_iri, SDO.additionalType, ACTISO.countryName))
            add_addr_component(country, ACTISO.countryName, country_iri, iri, graph)

            # unit_type_code
            unit_type_code = row[AddressTable.UNIT_TYPE_CODE]
            add_addr_component(
                unit_type_code,
                ADDRCMPType.flatTypeCode,
                flat_type_codes.get(
                    unit_type_code,
                    URIRef(f"https://example.com/flatTypeCode/{unit_type_code}"),
                ),
                iri,
                graph,
            )

            # unit_no
            unit_no = row[AddressTable.UNIT_NO]
            add_addr_component(
                unit_no,
                ADDRCMPType.flatNumber,
                Literal(unit_no),
                iri,
                graph,
            )

            # unit_suffix
            unit_suffix: str = row[AddressTable.UNIT_SUFFIX]
            add_addr_component(
                unit_suffix,
                ADDRCMPType.flatNumberSuffix,
                Literal(unit_suffix),
                iri,
                graph,
            )

            # level_type_code
            level_type_code = row[AddressTable.LEVEL_TYPE_CODE]
            add_addr_component(
                level_type_code,
                ADDRCMPType.levelTypeCode,
                level_type_codes.get(
                    level_type_code,
                    URIRef(f"https://example.com/levelTypeCode/{level_type_code}"),
                ),
                iri,
                graph,
            )

            # level_no
            level_no = row[AddressTable.LEVEL_NO]
            add_addr_component(
                level_no,
                ADDRCMPType.levelNumber,
                Literal(level_no),
                iri,
                graph,
            )

            # # street_no_first
            street_no_first = row[AddressTable.STREET_NO_FIRST]
            add_addr_component(
                street_no_first,
                ADDRCMPType.numberFirst,
                Literal(street_no_first),
                iri,
                graph,
            )

            # # street_no_first_suffix
            street_no_first_suffix = row[AddressTable.STREET_NO_FIRST_SUFFIX]
            add_addr_component(
                street_no_first_suffix,
                ADDRCMPType.numberFirstSuffix,
                Literal(street_no_first_suffix),
                iri,
                graph,
            )

            # street_no_last
            street_no_last = row[AddressTable.STREET_NO_LAST]
            add_addr_component(
                street_no_last,
                ADDRCMPType.numberLast,
                Literal(street_no_last),
                iri,
                graph,
            )

            # street_no_last_suffix
            street_no_last_suffix = row[AddressTable.STREET_NO_LAST_SUFFIX]
            add_addr_component(
                street_no_last_suffix,
                ADDRCMPType.numberLastSuffix,
                Literal(street_no_last_suffix),
                iri,
                graph,
            )

            # TODO: Provenance stuff here...

            # geocode_id
            geocode_iri = GeocodeTable.get_iri(row[GeocodeTable.GEOCODE_ID])
            graph.add((iri, ADDR.hasGeocode, geocode_iri))

            # road_id
            road_id = str(row[AddressTable.QRT_ROAD_ID])
            road_iri = QRTRoadsTable.get_iri(road_id)
            add_addr_component(
                road_id, ADDRCMPType.streetLocality, road_iri, iri, graph
            )

            # locality
            locality_id = str(row[AddressTable.LOCALITY_NAME])
            locality_iri = LocalityTable.get_iri(locality_id)
            add_addr_component(
                locality_id, ADDRCMPType.locality, locality_iri, iri, graph
            )

            # postcode
            postcode_component = BNode()
            graph.add((iri, SDO.hasPart, postcode_component))
            graph.add((postcode_component, SDO.additionalType, ACTISO.postcode))
            graph.add(
                (postcode_component, RDF.value, Literal(row[AddressTable.POSTCODE]))
            )

        Table.to_file(table_name, graph)
        graph.close()