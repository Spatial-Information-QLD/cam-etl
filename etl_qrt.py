import time
import concurrent.futures
from textwrap import dedent
from pathlib import Path

from rdflib import Dataset, Graph, URIRef, RDF, BNode, Literal, SDO, SKOS

from cam.etl import (
    add_additional_property,
    get_db_connection,
    get_concept_from_vocab,
    get_vocab_graph,
    worker_wrap,
    serialize,
)
from cam.etl.namespaces import (
    sir_id_datatype,
    CN,
    LC,
    ROADS,
    RNPT,
    lifecycle_stage_current,
)
from cam.etl.qrt import get_road_name_iri
from cam.etl.types import Row
from cam.etl.settings import settings


dataset_name = "qrt"
output_dir_name = "qrt-rdf"
graph_name = URIRef("urn:qali:graph:roads")

ROAD_TYPES_URL = "https://cdn.jsdelivr.net/gh/geological-survey-of-queensland/vocabularies@9fa34d76fc0a27d711d8030b934c2c83dd378156/vocabularies-qsi/road-types.ttl"
GN_AFFIX_URL = "https://cdn.jsdelivr.net/gh/geological-survey-of-queensland/vocabularies@b07763c87f2f872133197e6fb0eb911de85879c6/vocabularies-qsi/gn-affix.ttl"


def get_iri(road_id: str):
    return URIRef(f"https://linked.data.gov.au/dataset/qld-addr/road/{road_id}")


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
    road_name_full: str,
    road_name: str,
    road_type: str,
    road_suffix: str,
    # road_name_source: str,
    ds: Dataset,
    vocab_graph: Graph,
    row: Row,
    road_type_concept_scheme: URIRef,
    road_suffix_concept_scheme: URIRef,
):
    iri = get_iri(row[road_id])
    label_iri = get_road_name_iri(row[road_id])

    # Road Object
    ds.add((iri, RDF.type, ROADS.RoadObject, graph_name))
    ds.add(
        (
            iri,
            SDO.identifier,
            Literal(row[road_id], datatype=sir_id_datatype),
            graph_name,
        )
    )
    ds.add((iri, CN.hasName, label_iri, graph_name))

    # Road Label Lifecycle Stage
    bnode = BNode(f"{row[road_id]}-lifecycle-stage")
    ds.add((label_iri, LC.hasLifecycleStage, bnode, graph_name))
    ds.add((bnode, SDO.additionalType, lifecycle_stage_current, graph_name))

    # Name template
    ds.add(
        (
            label_iri,
            CN.nameTemplate,
            Literal(f"{{RNPT.RoadGivenName}} {{RNPT.RoadType}} {{RNPT.RoadSuffix}}"),
            graph_name,
        )
    )

    # Road Given Name
    road_name_value = row[road_name]
    if road_name_value:
        bnode = BNode(f"{row[road_id]}-road-name")
        ds.add((label_iri, SDO.hasPart, bnode, graph_name))
        ds.add(
            (
                bnode,
                SDO.additionalType,
                RNPT.roadGivenName,
                graph_name,
            )
        )
        ds.add((bnode, SDO.value, Literal(road_name_value), graph_name))

    # Road Type
    road_type_value = row[road_type]
    if road_type_value:
        bnode = BNode(f"{row[road_id]}-road-type")
        ds.add((label_iri, SDO.hasPart, bnode, graph_name))
        ds.add(
            (
                bnode,
                SDO.additionalType,
                RNPT.roadType,
                graph_name,
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
        ds.add((bnode, SDO.value, concept, graph_name))

    # Road Suffix
    road_suffix_value = row[road_suffix]
    if road_suffix_value:
        bnode = BNode(f"{row[road_id]}-road-suffix")
        ds.add((label_iri, SDO.hasPart, bnode, graph_name))
        ds.add(
            (
                bnode,
                SDO.additionalType,
                RNPT.roadSuffix,
                graph_name,
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
        ds.add((bnode, SDO.value, concept, graph_name))

    # Road Name
    ds.add((label_iri, RDF.type, ROADS.RoadName, graph_name))
    ds.add((label_iri, RDF.type, CN.CompoundName, graph_name))
    ds.add((label_iri, CN.isNameFor, iri, graph_name))
    ds.add((label_iri, SDO.name, Literal(row[road_name_full]), graph_name))


@worker_wrap
def worker(rows: list[Row], job_id: int, vocab_graph: Graph):
    ROAD_ID = "road_id_1"
    ROAD_NAME_FULL = "road_name_full_1"
    ROAD_NAME = "road_name_1"
    ROAD_TYPE = "road_type_1"
    ROAD_SUFFIX = "road_suffix_1"
    # ROAD_NAME_SOURCE = "road_name_1_source"

    ROAD_ID_2 = "road_id_2"
    ROAD_NAME_FULL_2 = "road_name_full_2"
    ROAD_NAME_2 = "road_name_2"
    ROAD_TYPE_2 = "road_type_2"
    ROAD_SUFFIX_2 = "road_suffix_2"
    # ROAD_NAME_SOURCE_2 = "road_name_2_source"

    road_type_concept_scheme = URIRef("https://linked.data.gov.au/def/road-types")
    road_suffix_concept_scheme = URIRef("https://linked.data.gov.au/def/gn-affix")

    ds = Dataset(store="Oxigraph")
    for row in rows:
        transform_row(
            ROAD_ID,
            ROAD_NAME_FULL,
            ROAD_NAME,
            ROAD_TYPE,
            ROAD_SUFFIX,
            # ROAD_NAME_SOURCE,
            ds,
            vocab_graph,
            row,
            road_type_concept_scheme,
            road_suffix_concept_scheme,
        )
        # if row[ROAD_NAME_FULL_2]:
        #     transform_row(
        #         ROAD_ID_2,
        #         ROAD_NAME_FULL_2,
        #         ROAD_NAME_2,
        #         ROAD_TYPE_2,
        #         ROAD_SUFFIX_2,
        #         ROAD_NAME_SOURCE_2,
        #         ds,
        #         vocab_graph,
        #         row,
        #         road_type_concept_scheme,
        #         road_suffix_concept_scheme,
        #     )

    output_dir = Path(output_dir_name)
    filename = Path(dataset_name + "-" + str(job_id) + ".nq")
    serialize(output_dir, str(filename), ds)


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
                    SELECT DISTINCT
                        q.road_id as road_id_1,
                        q.road_name_ as road_name_full_1,
                        q.road_name as road_name_1,
                        q.road_type as road_type_1,
                        q.road_suffi as road_suffix_1
                    FROM qrt_spatial q
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
