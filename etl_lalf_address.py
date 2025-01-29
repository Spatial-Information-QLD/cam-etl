import time
import concurrent.futures
from textwrap import dedent
from pathlib import Path

from rdflib import (
    BNode,
    Dataset,
    Graph,
    URIRef,
    RDF,
    RDFS,
    Literal,
    SDO,
    TIME,
    XSD,
    SKOS,
)

from cam.etl import (
    get_db_connection,
    get_vocab_graph,
    worker_wrap,
    serialize,
)
from cam.etl.lalf_address import (
    get_address_iri,
    addr_status_vocab_mapping,
    get_address_uuid,
    addr_level_type_vocab_mapping,
)
from cam.etl.lalf_parcel import get_parcel_iri
from cam.etl.namespaces import (
    ADDR,
    CN,
    LC,
    sir_id_datatype,
    lifecycle_stage_current,
    ADDR_PT,
    aus_country,
    qld_state,
)
from cam.etl.pndb import get_geographical_name_iri
from cam.etl.qrt import get_road_label_iri
from cam.etl.types import Row
from cam.etl.settings import settings

dataset_name = "lalf_address"
output_dir_name = "lalf-rdf"
graph_name = URIRef("urn:ladb:graph:addresses")

SUB_ADDRESS_TYPES_VOCAB_URL = "https://cdn.jsdelivr.net/gh/icsm-au/icsm-vocabs@2776af7d25b1484b9f7c2886adf5667231deb6ad/vocabs/Addresses/addr-subaddress-types.ttl"
LEVEL_TYPES_VOCAB_URL = "https://cdn.jsdelivr.net/gh/icsm-au/icsm-vocabs@6d2b90a4acb306791922d4649914a03cae5d019d/vocabs/Addresses/addr-level-types.ttl"

address_type_street_iri = URIRef(
    "https://linked.data.gov.au/def/address-classes/street"
)

ADDR_ID = "addr_id"
LOT_NO = "lot_no"
PLAN_NO = "plan_no"
ADDR_STATUS_CODE = "addr_status_code"
ADDR_CREATE_DATE = "addr_create_date"
LOCALITY_REF_NO = "locality_ref_no"
LOCALITY_NAME = "locality_name"
ROAD_ID = "road_id_1"
ROAD_NAME_FULL_1 = "road_name_full_1"
STREET_NO_LAST_SUFFIX = "street_no_last_suffix"
STREET_NO_LAST = "street_no_last"
STREET_NO_FIRST_SUFFIX = "street_no_first_suffix"
STREET_NO_FIRST = "street_no_first"
UNIT_SUFFIX = "unit_suffix"
UNIT_NO = "unit_no"
UNIT_TYPE_CODE = "unit_type_code"
LEVEL_SUFFIX = "level_suffix"
LEVEL_NO = "level_no"
LEVEL_TYPE_CODE = "level_type_code"


@worker_wrap
def worker(
    rows: list[Row],
    job_id: int,
    sub_address_types_graph: Graph,
    level_types_graph: Graph,
):
    ds = Dataset(store="Oxigraph")

    for row in rows:
        # use this for bnode identifiers that are compatible with oxigraph
        addr_id_uuid = get_address_uuid(row[ADDR_ID])

        # address
        addr_iri = get_address_iri(row[ADDR_ID])
        ds.add((addr_iri, RDF.type, ADDR.Address, graph_name))
        ds.add((addr_iri, RDF.type, CN.CompoundName, graph_name))
        ds.add(
            (
                addr_iri,
                SDO.identifier,
                Literal(row[ADDR_ID], datatype=sir_id_datatype),
                graph_name,
            )
        )

        # link to parcel
        parcel_iri = get_parcel_iri(row[LOT_NO], row[PLAN_NO])
        ds.add((addr_iri, CN.isNameFor, parcel_iri, graph_name))
        ds.add((parcel_iri, CN.hasName, addr_iri, graph_name))

        # address status
        address_status = row[ADDR_STATUS_CODE]
        addr_status_iri = addr_status_vocab_mapping[address_status]
        ds.add((addr_iri, ADDR.hasStatus, addr_status_iri, graph_name))

        # address type - street
        ds.add((addr_iri, SDO.additionalType, address_type_street_iri, graph_name))

        # lifecycle stage
        if addr_create_date := row[ADDR_CREATE_DATE]:
            lifecycle_node = BNode(f"{addr_id_uuid}-lifecycle")
            lifecycle_time_node = BNode(f"{addr_id_uuid}-lifecycle-time")
            ds.add((addr_iri, LC.hasLifecycleStage, lifecycle_node, graph_name))
            ds.add((lifecycle_node, TIME.hasBeginning, lifecycle_time_node, graph_name))
            ds.add(
                (
                    lifecycle_time_node,
                    TIME.inXSDDateTime,
                    Literal(addr_create_date, datatype=XSD.dateTime),
                    graph_name,
                )
            )
            ds.add(
                (
                    lifecycle_node,
                    SDO.additionalType,
                    lifecycle_stage_current,
                    graph_name,
                )
            )

        # country
        country_node = BNode(f"{addr_id_uuid}-country")
        ds.add((addr_iri, SDO.hasPart, country_node, graph_name))
        ds.add((country_node, SDO.additionalType, ADDR_PT.countryName, graph_name))
        ds.add((country_node, SDO.value, aus_country, graph_name))

        # state
        state_node = BNode(f"{addr_id_uuid}-state")
        ds.add((addr_iri, SDO.hasPart, state_node, graph_name))
        ds.add((state_node, SDO.additionalType, ADDR_PT.stateOrTerritory, graph_name))
        ds.add((state_node, SDO.value, qld_state, graph_name))

        # postcode
        # TODO:

        # locality
        locality_iri = get_geographical_name_iri(row[LOCALITY_REF_NO])
        locality_node = BNode(f"{addr_id_uuid}-locality")
        ds.add((addr_iri, SDO.hasPart, locality_node, graph_name))
        ds.add((locality_node, SDO.additionalType, ADDR_PT.geographicName, graph_name))
        ds.add((locality_node, SDO.value, locality_iri, graph_name))

        # street
        street_iri = get_road_label_iri(row[ROAD_ID])
        road_node = BNode(f"{addr_id_uuid}-road")
        ds.add((addr_iri, SDO.hasPart, road_node, graph_name))
        ds.add((road_node, SDO.additionalType, ADDR_PT.road, graph_name))
        ds.add((road_node, SDO.value, street_iri, graph_name))

        # street no last suffix
        if street_no_last_suffix := row[STREET_NO_LAST_SUFFIX]:
            street_no_last_suffix_node = BNode(f"{addr_id_uuid}-street-no-last-suffix")
            ds.add((addr_iri, SDO.hasPart, street_no_last_suffix_node, graph_name))
            ds.add(
                (
                    street_no_last_suffix_node,
                    SDO.additionalType,
                    ADDR_PT.addressNumberLastSuffix,
                    graph_name,
                )
            )
            ds.add(
                (
                    street_no_last_suffix_node,
                    SDO.value,
                    Literal(street_no_last_suffix),
                    graph_name,
                )
            )

        # street no last
        if street_no_last := row[STREET_NO_LAST]:
            street_no_last_node = BNode(f"{addr_id_uuid}-street-no-last")
            ds.add((addr_iri, SDO.hasPart, street_no_last_node, graph_name))
            ds.add(
                (
                    street_no_last_node,
                    SDO.additionalType,
                    ADDR_PT.addressNumberLast,
                    graph_name,
                )
            )
            ds.add(
                (street_no_last_node, SDO.value, Literal(street_no_last), graph_name)
            )

        # street no first suffix
        if street_no_first_suffix := row[STREET_NO_FIRST_SUFFIX]:
            street_no_first_suffix_node = BNode(
                f"{addr_id_uuid}-street-no-first-suffix"
            )
            ds.add((addr_iri, SDO.hasPart, street_no_first_suffix_node, graph_name))
            ds.add(
                (
                    street_no_first_suffix_node,
                    SDO.additionalType,
                    ADDR_PT.addressNumberFirstSuffix,
                    graph_name,
                )
            )
            ds.add(
                (
                    street_no_first_suffix_node,
                    SDO.value,
                    Literal(street_no_first_suffix),
                    graph_name,
                )
            )

        # street no first
        if street_no_first := row[STREET_NO_FIRST]:
            street_no_first_node = BNode(f"{addr_id_uuid}-street-no-first")
            ds.add((addr_iri, SDO.hasPart, street_no_first_node, graph_name))
            ds.add(
                (
                    street_no_first_node,
                    SDO.additionalType,
                    ADDR_PT.addressNumberFirst,
                    graph_name,
                )
            )
            ds.add(
                (street_no_first_node, SDO.value, Literal(street_no_first), graph_name)
            )

        # sub-address number suffix
        if unit_suffix := row[UNIT_SUFFIX]:
            unit_suffix_node = BNode(f"{addr_id_uuid}-unit-suffix")
            ds.add((addr_iri, SDO.hasPart, unit_suffix_node, graph_name))
            ds.add(
                (
                    unit_suffix_node,
                    SDO.additionalType,
                    ADDR_PT.subaddressNumberSuffix,
                    graph_name,
                )
            )
            ds.add((unit_suffix_node, SDO.value, Literal(unit_suffix), graph_name))

        # sub-address number
        if unit_no := row[UNIT_NO]:
            unit_no_node = BNode(f"{addr_id_uuid}-unit-no")
            ds.add((addr_iri, SDO.hasPart, unit_no_node, graph_name))
            ds.add(
                (unit_no_node, SDO.additionalType, ADDR_PT.subaddressNumber, graph_name)
            )
            ds.add((unit_no_node, SDO.value, Literal(unit_no), graph_name))

        # sub-address type
        if unit_type_code := row[UNIT_TYPE_CODE]:
            unit_type_code_node = BNode(f"{addr_id_uuid}-unit-type-code")
            unit_type_iri = sub_address_types_graph.value(
                predicate=SKOS.altLabel, object=Literal(unit_type_code, lang="en")
            )
            if unit_type_iri is None:
                raise Exception(
                    f"No sub-address type concept matched for value {unit_type_code}."
                )
            ds.add((addr_iri, SDO.hasPart, unit_type_code_node, graph_name))
            ds.add(
                (
                    unit_type_code_node,
                    SDO.additionalType,
                    ADDR_PT.subaddressType,
                    graph_name,
                )
            )
            ds.add((unit_type_code_node, SDO.value, unit_type_iri, graph_name))
            unit_type_label = sub_address_types_graph.value(
                unit_type_iri, SKOS.prefLabel
            )
            if unit_type_label is None:
                raise Exception("No unit type label found for value {unit_type_code}")
            unit_type_label = str(unit_type_label)

        # building level number suffix
        if level_suffix := row[LEVEL_SUFFIX]:
            level_suffix_node = BNode(f"{addr_id_uuid}-level-suffix")
            ds.add((addr_iri, SDO.hasPart, level_suffix_node, graph_name))
            ds.add(
                (
                    level_suffix_node,
                    SDO.additionalType,
                    ADDR_PT.buildingLevelNumberSuffix,
                    graph_name,
                )
            )
            ds.add((level_suffix_node, SDO.value, Literal(level_suffix), graph_name))

        # building level number
        if level_no := row[LEVEL_NO]:
            level_no_node = BNode(f"{addr_id_uuid}-level-no")
            ds.add((addr_iri, SDO.hasPart, level_no_node, graph_name))
            ds.add(
                (
                    level_no_node,
                    SDO.additionalType,
                    ADDR_PT.buildingLevelNumber,
                    graph_name,
                )
            )
            ds.add((level_no_node, SDO.value, Literal(level_no), graph_name))

        # building level type
        if level_type_code := row[LEVEL_TYPE_CODE]:
            level_type_code_node = BNode(f"{addr_id_uuid}-level-type-code")
            if level_type_code == "L":
                level_type_iri = addr_level_type_vocab_mapping[level_type_code]
            else:
                level_type_iri = level_types_graph.value(
                    predicate=SKOS.altLabel, object=Literal(level_type_code, lang="en")
                )
            if level_type_iri is None:
                raise Exception(
                    f"No level type concept matched for value {level_type_code}."
                )
            ds.add((addr_iri, SDO.hasPart, level_type_code_node, graph_name))
            ds.add(
                (
                    level_type_code_node,
                    SDO.additionalType,
                    ADDR_PT.buildingLevelType,
                    graph_name,
                )
            )
            ds.add((level_type_code_node, SDO.value, level_type_iri, graph_name))
            level_type_label = level_types_graph.value(level_type_iri, SKOS.prefLabel)
            if level_type_label is None:
                raise Exception(
                    f"No level type label found for value {level_type_code}"
                )
            level_type_label = str(level_type_label)

        # geographical name
        # TODO:

        # rdfs:label
        label = f"{level_type_label if level_type_code else ''} {level_no}{level_suffix} {unit_type_label + ' ' if unit_type_code else ''}{unit_no}{unit_suffix}{'/' if unit_no else ''}{street_no_first}{'-' + street_no_last if street_no_last else ''}{street_no_last_suffix} {row[ROAD_NAME_FULL_1]}, {row[LOCALITY_NAME]}, Queensland, Australia".strip()
        ds.add((addr_iri, RDFS.label, Literal(label, datatype=XSD.string), graph_name))

    output_dir = Path(output_dir_name)
    filename = Path(dataset_name + "-" + str(job_id) + ".nq")
    serialize(output_dir, str(filename), ds)


def main():
    start_time = time.time()

    sub_address_types_graph = get_vocab_graph([SUB_ADDRESS_TYPES_VOCAB_URL])
    level_types_graph = get_vocab_graph([LEVEL_TYPES_VOCAB_URL])
    print(
        f"Remotely fetched {len(sub_address_types_graph)} statements for sub_address_types_graph"
    )
    print(f"Remotely fetched {len(level_types_graph)} statements for level_types_graph")

    with get_db_connection(
        host=settings.etl.db.host,
        port=settings.etl.db.port,
        dbname=settings.etl.db.name,
        user=settings.etl.db.user,
        password=settings.etl.db.password,
    ) as connection:

        with connection.cursor(name="main", scrollable=False) as cursor:
            cursor.itersize = settings.etl.batch_size
            cursor.execute(
                dedent(
                    """\
                    WITH qrt_road AS (
                        SELECT DISTINCT road_name_basic_1, locality_left, road_id_1, road_name_full_1
                        FROM qrt
                    )
                    
                    SELECT g.geocode_id, p.lot_no, l."pndb.ref_no" as locality_ref_no, l."pndb.place_name" as locality_name, q.road_id_1, q.road_name_basic_1, q.road_name_full_1, p.plan_no, a.*
                    FROM "lalfpdba.lf_address" a
                    JOIN "lalfpdba.lf_site" s ON a.site_id = s.site_id
                    JOIN "lalfpdba.lf_parcel" p ON s.parcel_id = p.parcel_id
                    LEFT JOIN "lalfpdba.lf_geocode" g ON s.site_id = g.site_id
                    LEFT JOIN "lalfpdba.lf_road" r ON r.road_id = a.road_id
                    LEFT JOIN lalf_pndb_localities_joined l ON r.locality_code = l."lalf.locality_code"
                    LEFT JOIN qrt_road q ON r.qrt_road_name_basic = q.road_name_basic_1
                      AND l."pndb.place_name" = q.locality_left
                    WHERE a.addr_status_code != 'H'
                """
                ),
            )

            with concurrent.futures.ProcessPoolExecutor() as executor:
                futures = []
                while True:
                    rows = cursor.fetchmany(settings.etl.batch_size)
                    if not rows:
                        break

                    job_id = len(futures) + 1
                    futures.append(
                        executor.submit(
                            worker,
                            rows,
                            job_id,
                            sub_address_types_graph,
                            level_types_graph,
                        )
                    )

                for future in concurrent.futures.as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        print(f"A worker process failed with error: {e}")
                        for f in futures:
                            f.cancel()
                        raise

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Total execution time: {execution_time:.2f} seconds")


if __name__ == "__main__":
    main()
