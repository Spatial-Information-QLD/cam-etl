import itertools
from pathlib import Path

from rdflib import Graph, URIRef, BNode, Literal
from rdflib.namespace import RDF, SDO, SKOS
from pyspark.sql import SparkSession
from jinja2 import Template

from cam.tables import Table
from cam.tables.lf_parcel import ParcelTable
from cam.tables.lf_geocode import GeocodeTable
from cam.tables.qrt import QRTRoadsTable
from cam.tables.locality import LocalityTable
from cam.tables.lf_place_name import PlacenameTable
from cam.tables.lf_unit_type import UnitTypeTable
from cam.tables.lf_level_type import LevelTypeTable
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
                    select distinct
                        p.parcel_id,
                        pn.pl_name_id,
                        a.*,
                        q.road_id as qrt_road_id,
                        l.locality_name,
                        l.state,
                        po.postcode
                    from lalfdb.lalfpdba_lf_site s
                        left join lalfdb.lalfpdba_lf_place_name pn on pn.site_id = s.site_id
                        join lalfdb.lalfpdba_lf_parcel p on p.parcel_id = s.parcel_id
                        join lalfdb.lalfpdba_lf_address a on a.site_id = s.site_id
                        join lalfdb.lalfpdba_lf_geocode g on g.site_id = s.site_id
                        join lalfdb.lalfpdba_sp_survey_point sp on sp.pid = g.spdb_pid
                        join lalfdb.qld_postcode_polygon pp on lalfdb.st_intersects(pp.wkt, lalfdb.st_geomfromtext(sp.wkt_literal, 4326))
                        join lalfdb.postcode po on po.pc_pid = pp.pc_pid
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

        PARCEL_ID = "parcel_id"
        ADDR_ID = "addr_id"
        ADDR_STATUS_CODE = "addr_status_code"
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
        GEOCODE_ID = "geocode_id"
        PLACE_NAME_ID = "pl_name_id"

        # Fetch FSDF vocabularies
        # flat_type_codes = get_remote_concepts(
        #     FSDF_SPARQL_ENDPOINT,
        #     FLAT_TYPE_CODE_IRI,
        # )
        # level_type_codes = get_remote_concepts(
        #     FSDF_SPARQL_ENDPOINT, LEVEL_TYPE_CODE_IRI
        # )

        # Process each row in the results
        for row in rows:
            # addr_id
            iri = AddressTable.get_iri(row[ADDR_ID])
            graph.add((iri, RDF.type, ADDR.Address))
            graph.add((iri, RDF.type, CN.CompoundName))

            parcel_iri = ParcelTable.get_iri(row[PARCEL_ID])
            graph.add((iri, CN.isNameFor, parcel_iri))
            graph.add((parcel_iri, SDO.name, iri))

            # address status and relationship to addressable object
            graph.add((parcel_iri, ADDR.hasAddress, iri))
            graph.add((iri, ADDR.isAddressFor, parcel_iri))
            status = row[ADDR_STATUS_CODE]
            if status == "P":
                graph.add((parcel_iri, ADDR.hasPrimary, iri))
                graph.add((iri, ADDR.isPrimaryAddressFor, parcel_iri))
            elif status == "A":
                graph.add((parcel_iri, ADDR.hasAlias, iri))
                graph.add((iri, ADDR.isAliasAddressFor, parcel_iri))

            # Create and add QLD state
            state = row[STATE]
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
            unit_type_code = row[UNIT_TYPE_CODE]
            add_addr_component(
                unit_type_code,
                ADDRCMPType.flatTypeCode,
                UnitTypeTable.get_iri(unit_type_code),
                iri,
                graph,
            )

            # unit_no
            unit_no = row[UNIT_NO]
            add_addr_component(
                unit_no,
                ADDRCMPType.flatNumber,
                Literal(unit_no),
                iri,
                graph,
            )

            # unit_suffix
            unit_suffix: str = row[UNIT_SUFFIX]
            add_addr_component(
                unit_suffix,
                ADDRCMPType.flatNumberSuffix,
                Literal(unit_suffix),
                iri,
                graph,
            )

            # level_type_code
            level_type_code = row[LEVEL_TYPE_CODE]
            add_addr_component(
                level_type_code,
                ADDRCMPType.levelTypeCode,
                URIRef(LevelTypeTable.get_iri(level_type_code)),
                iri,
                graph,
            )

            # level_no
            level_no = row[LEVEL_NO]
            add_addr_component(
                level_no,
                ADDRCMPType.levelNumber,
                Literal(level_no),
                iri,
                graph,
            )

            # # street_no_first
            street_no_first = row[STREET_NO_FIRST]
            add_addr_component(
                street_no_first,
                ADDRCMPType.numberFirst,
                Literal(street_no_first),
                iri,
                graph,
            )

            # # street_no_first_suffix
            street_no_first_suffix = row[STREET_NO_FIRST_SUFFIX]
            add_addr_component(
                street_no_first_suffix,
                ADDRCMPType.numberFirstSuffix,
                Literal(street_no_first_suffix),
                iri,
                graph,
            )

            # street_no_last
            street_no_last = row[STREET_NO_LAST]
            add_addr_component(
                street_no_last,
                ADDRCMPType.numberLast,
                Literal(street_no_last),
                iri,
                graph,
            )

            # street_no_last_suffix
            street_no_last_suffix = row[STREET_NO_LAST_SUFFIX]
            add_addr_component(
                street_no_last_suffix,
                ADDRCMPType.numberLastSuffix,
                Literal(street_no_last_suffix),
                iri,
                graph,
            )

            # geocode_id
            geocode_iri = GeocodeTable.get_iri(row[GEOCODE_ID])
            graph.add((iri, ADDR.hasGeocode, geocode_iri))

            # road_id
            road_id = str(row[QRT_ROAD_ID])
            road_iri = QRTRoadsTable.get_iri(road_id)
            add_addr_component(
                road_id, ADDRCMPType.streetLocality, road_iri, iri, graph
            )

            # locality
            locality_id = str(row[LOCALITY_NAME])
            locality_iri = LocalityTable.get_iri(locality_id)
            add_addr_component(
                locality_id, ADDRCMPType.locality, locality_iri, iri, graph
            )

            # postcode
            postcode_component = BNode()
            graph.add((iri, SDO.hasPart, postcode_component))
            graph.add((postcode_component, SDO.additionalType, ACTISO.postcode))
            graph.add((postcode_component, RDF.value, Literal(row[POSTCODE])))

            # place name
            place_name_id = (
                str(row[PLACE_NAME_ID]) if row[PLACE_NAME_ID] is not None else None
            )
            place_name_iri = PlacenameTable.get_iri(place_name_id)
            add_addr_component(
                place_name_id, ADDRCMPType.placeName, place_name_iri, iri, graph
            )

        Table.to_file(table_name, graph)
        graph.close()
