import time
import concurrent.futures
from textwrap import dedent
from pathlib import Path

from rdflib import Graph, URIRef, RDF, BNode, Literal, SDO, SKOS

from cam.etl import (
    add_additional_property,
    get_db_connection,
    get_concept_from_vocab,
    get_vocab_graph,
    worker_wrap,
    serialize,
)
from cam.etl.namespaces import sir_id_datatype, CN, LC, REG, ROADS, RNPT
from cam.etl.types import Row
from cam.etl.settings import settings


dataset = "qrt"
output_dir_name = "qrt-rdf"

ROAD_TYPES_URL = "https://cdn.jsdelivr.net/gh/geological-survey-of-queensland/vocabularies@9fa34d76fc0a27d711d8030b934c2c83dd378156/vocabularies-qsi/road-types.ttl"
GN_AFFIX_URL = "https://cdn.jsdelivr.net/gh/geological-survey-of-queensland/vocabularies@b07763c87f2f872133197e6fb0eb911de85879c6/vocabularies-qsi/gn-affix.ttl"


def get_iri(road_id: str):
    return URIRef(f"https://linked.data.gov.au/dataset/qld-addr/road/{road_id}")


def get_segment_iri(segment_id: str):
    return URIRef(
        f"https://linked.data.gov.au/dataset/qld-addr/road-segment/{segment_id}"
    )


def get_label_iri(road_id: str):
    return URIRef(f"https://linked.data.gov.au/dataset/qld-addr/road-label/{road_id}")


def get_locality_iri(value: str):
    return URIRef(
        f"https://linked.data.gov.au/dataset/qld-addr/locality/{value.lower().replace(' ', '-')}"
    )


def get_lga_iri(value: str):
    return URIRef(
        f"https://linked.data.gov.au/dataset/qld-addr/lga/{value.lower().replace(' ', '-')}"
    )


def transform_row(
    road_id: str,
    road_segment_id: str,
    locality_left: str,
    locality_right: str,
    lga_name_left: str,
    lga_name_right: str,
    road_name_full: str,
    road_name: str,
    road_type: str,
    road_suffix: str,
    road_name_basic: str,
    road_name_source: str,
    graph: Graph,
    vocab_graph: Graph,
    row: Row,
    road_type_concept_scheme: URIRef,
    road_suffix_concept_scheme: URIRef,
):
    iri = get_iri(row[road_id])
    segment_iri = get_segment_iri(row[road_segment_id])
    label_iri = get_label_iri(row[road_id])

    # Road Object
    graph.add((iri, RDF.type, ROADS.RoadObject))
    graph.add((iri, SDO.identifier, Literal(row[road_id], datatype=sir_id_datatype)))
    graph.add((iri, SDO.hasPart, segment_iri))
    graph.add((iri, SDO.name, label_iri))

    # Road Segment
    graph.add((segment_iri, RDF.type, ROADS.RoadSegment))
    graph.add(
        (
            segment_iri,
            SDO.identifier,
            Literal(row[road_segment_id], datatype=sir_id_datatype),
        )
    )
    graph.add((segment_iri, SDO.isPartOf, iri))

    add_additional_property(segment_iri, locality_left, row[locality_left], graph)
    add_additional_property(segment_iri, locality_right, row[locality_right], graph)
    add_additional_property(segment_iri, lga_name_left, row[lga_name_left], graph)
    add_additional_property(segment_iri, lga_name_right, row[lga_name_right], graph)

    locality_left_iri = get_locality_iri(row[locality_left])
    graph.add((segment_iri, ROADS.localityLeft, locality_left_iri))
    graph.add((locality_left_iri, SDO.name, Literal(row[locality_left])))

    locality_right_iri = get_locality_iri(row[locality_right])
    graph.add((segment_iri, ROADS.localityRight, locality_right_iri))
    graph.add((locality_right_iri, SDO.name, Literal(row[locality_right])))

    lga_left_iri = get_lga_iri(row[lga_name_left])
    graph.add((segment_iri, ROADS.lgaLeft, lga_left_iri))
    graph.add((lga_left_iri, SDO.name, Literal(row[lga_name_left])))

    lga_right_iri = get_lga_iri(row[lga_name_right])
    graph.add((segment_iri, ROADS.lgaRight, lga_right_iri))
    graph.add((lga_right_iri, SDO.name, Literal(row[lga_name_right])))

    # Road Label
    graph.add((label_iri, RDF.type, ROADS.RoadLabel))
    graph.add((label_iri, RDF.type, CN.CompoundName))
    graph.add((label_iri, CN.isNameFor, iri))
    graph.add((label_iri, SDO.name, Literal(row[road_name_full])))
    add_additional_property(label_iri, road_name_basic, row[road_name_basic], graph)
    add_additional_property(label_iri, road_name_source, row[road_name_source], graph)

    # TODO: add authority

    # Road Label Lifecycle Stage
    bnode = BNode(f"{row[road_id]}-lifecycle-stage")
    graph.add((label_iri, LC.hasLifecycleStage, bnode))
    graph.add((bnode, SDO.additionalType, REG.accepted))

    # Name template
    graph.add(
        (
            label_iri,
            CN.nameTemplate,
            Literal(f"{{RNPT.RoadGivenName}} {{RNPT.RoadType}} {{RNPT.RoadSuffix}}"),
        )
    )

    # Road Given Name
    road_name_value = row[road_name]
    if road_name_value:
        bnode = BNode(f"{row[road_id]}-road-name")
        graph.add((label_iri, SDO.hasPart, bnode))
        graph.add(
            (
                bnode,
                SDO.additionalType,
                RNPT.RoadGivenName,
            )
        )
        graph.add((bnode, SDO.value, Literal(road_name_value)))

    # Road Type
    road_type_value = row[road_type]
    if road_type_value:
        bnode = BNode(f"{row[road_id]}-road-type")
        graph.add((label_iri, SDO.hasPart, bnode))
        graph.add(
            (
                bnode,
                SDO.additionalType,
                RNPT.RoadType,
            )
        )

        if road_type_value == "Island":
            # TODO: island is missing in the vocab.
            concept = URIRef("https://linked.data.gov.au/def/road-types/island")
        elif road_type_value == "Linkway":
            # TODO: linkway is missing in the vocab.
            concept = URIRef("https://linked.data.gov.au/def/road-types/linkway")
        elif road_type_value == "Perch":
            # TODO: perch is missing in the vocab.
            concept = URIRef("https://linked.data.gov.au/def/road-types/perch")
        elif road_type_value == "Nest":
            # TODO: nest is missing in the vocab.
            concept = URIRef("https://linked.data.gov.au/def/road-types/nest")
        elif road_type_value == "Yards":
            # Map to Yard.
            concept = URIRef("https://linked.data.gov.au/def/road-types/yard")
        elif road_type_value == "St":
            # Map to Street.
            concept = URIRef("https://linked.data.gov.au/def/road-types/street")
        else:
            concept = get_concept_from_vocab(
                SKOS.prefLabel,
                Literal(road_type_value, lang="en"),
                road_type_concept_scheme,
                vocab_graph,
            )
            if not concept:
                raise Exception(
                    f"Concept IRI not found for road type value '{road_type_value}'"
                )
        graph.add((bnode, SDO.value, concept))

    # Road Suffix
    road_suffix_value = row[road_suffix]
    if road_suffix_value:
        bnode = BNode(f"{row[road_id]}-road-suffix")
        graph.add((label_iri, SDO.hasPart, bnode))
        graph.add(
            (
                bnode,
                SDO.additionalType,
                RNPT.RoadSuffix,
            )
        )
        concept = get_concept_from_vocab(
            SKOS.altLabel,
            Literal(road_suffix_value, lang="en"),
            road_suffix_concept_scheme,
            vocab_graph,
        )
        if not concept:
            raise Exception(
                f"Concept IRI not found for road suffix value '{road_suffix_value}'"
            )
        graph.add((bnode, SDO.value, concept))


@worker_wrap
def worker(rows: list[Row], job_id: int, vocab_graph: Graph):
    ROAD_SEGMENT_ID = "segment_id"
    LOCALITY_LEFT = "locality_left"
    LOCALITY_RIGHT = "locality_right"
    LGA_NAME_LEFT = "lga_name_left"
    LGA_NAME_RIGHT = "lga_name_right"

    ROAD_ID = "road_id_1"
    ROAD_NAME_FULL = "road_name_full_1"
    ROAD_NAME = "road_name_1"
    ROAD_TYPE = "road_type_1"
    ROAD_SUFFIX = "road_suffix_1"
    ROAD_NAME_BASIC = "road_name_basic_1"
    ROAD_NAME_SOURCE = "road_name_1_source"

    ROAD_ID_2 = "road_id_2"
    ROAD_NAME_FULL_2 = "road_name_full_2"
    ROAD_NAME_2 = "road_name_2"
    ROAD_TYPE_2 = "road_type_2"
    ROAD_SUFFIX_2 = "road_suffix_2"
    ROAD_NAME_BASIC_2 = "road_name_basic_2"
    ROAD_NAME_SOURCE_2 = "road_name_2_source"

    road_type_concept_scheme = URIRef("https://linked.data.gov.au/def/road-types")
    road_suffix_concept_scheme = URIRef("https://linked.data.gov.au/def/gn-affix")

    graph = Graph(store="Oxigraph")
    for row in rows:
        transform_row(
            ROAD_ID,
            ROAD_SEGMENT_ID,
            LOCALITY_LEFT,
            LOCALITY_RIGHT,
            LGA_NAME_LEFT,
            LGA_NAME_RIGHT,
            ROAD_NAME_FULL,
            ROAD_NAME,
            ROAD_TYPE,
            ROAD_SUFFIX,
            ROAD_NAME_BASIC,
            ROAD_NAME_SOURCE,
            graph,
            vocab_graph,
            row,
            road_type_concept_scheme,
            road_suffix_concept_scheme,
        )
        if row[ROAD_NAME_FULL_2]:
            transform_row(
                ROAD_ID_2,
                ROAD_SEGMENT_ID,
                LOCALITY_LEFT,
                LOCALITY_RIGHT,
                LGA_NAME_LEFT,
                LGA_NAME_RIGHT,
                ROAD_NAME_FULL_2,
                ROAD_NAME_2,
                ROAD_TYPE_2,
                ROAD_SUFFIX_2,
                ROAD_NAME_BASIC_2,
                ROAD_NAME_SOURCE_2,
                graph,
                vocab_graph,
                row,
                road_type_concept_scheme,
                road_suffix_concept_scheme,
            )

    output_dir = Path(output_dir_name)
    filename = Path(dataset + "-" + str(job_id) + ".nt")
    serialize(output_dir, filename, graph)


def main():
    start_time = time.time()

    vocab_graph = get_vocab_graph(
        [
            ROAD_TYPES_URL,
            GN_AFFIX_URL,
        ]
    )
    print(f"Remotely fetched {len(vocab_graph)} statements for vocab_graph")

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
                    SELECT
                        *
                    FROM
                        "qrt" q
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
                    futures.append(executor.submit(worker, rows, job_id, vocab_graph))

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
