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
    Literal,
    SDO,
    SKOS,
)

from cam.etl import (
    get_db_connection,
    get_vocab_graph,
    worker_wrap,
    serialize,
    add_additional_property,
    get_concept_from_vocab,
)
from cam.etl.namespaces import (
    CN,
    LC,
    qrt_datatype,
    lifecycle_stage_current,
    ROADS,
    RNPT,
)
from cam.etl.qrt import get_road_name_iri, get_road_object_iri
from cam.etl.types import Row
from cam.etl.settings import settings

dataset_name = "lalf_road_missing_qrt"
output_dir_name = "lalf-rdf"
road_graph_name = URIRef("urn:qali:graph:roads")

ROAD_NAME_TYPES_VOCAB_URL = "https://cdn.jsdelivr.net/gh/icsm-au/icsm-vocabs@c0b59a51f09d1ae4b0a337f0a10747c45b1d44d3/vocabs/TransportNetworks/road-types.ttl"
ROAD_NAME_SUFFIXES_VOCAB_URL = "https://cdn.jsdelivr.net/gh/icsm-au/icsm-vocabs@f5650d07288b4879620be924042c73b6dae881e3/vocabs/GeographicalNames/gn-affix.ttl"

ROAD_ID = "road_id"
ROAD_NAME = "road_name"
ROAD_NAME_TYPE_CODE = "road_name_type_code"
ROAD_NAME_SUFFIX_CODE = "road_name_suffix_code"
ROAD_NAME_FULL = "road_name_full"

road_type_concept_scheme = URIRef("https://linked.data.gov.au/def/road-types")
road_suffix_concept_scheme = URIRef("https://linked.data.gov.au/def/gn-affix")


@worker_wrap
def worker(
    rows: list[Row],
    job_id: int,
    road_name_types_graph: Graph,
    road_name_suffixes_graph: Graph,
):
    ds = Dataset(store="Oxigraph")

    for road_row in rows:
        road_id = road_row[ROAD_ID]
        road_iri = get_road_name_iri(road_id)

        # Road object
        road_object_iri = get_road_object_iri(road_id)
        ds.add((road_object_iri, RDF.type, ROADS.RoadObject, road_graph_name))
        ds.add(
            (
                road_object_iri,
                SDO.identifier,
                Literal(road_id, datatype=qrt_datatype),
                road_graph_name,
            )
        )
        ds.add((road_object_iri, CN.hasName, road_iri, road_graph_name))

        # Additional property - missing QRT road
        add_additional_property(
            road_iri,
            "missing_qrt_road",
            True,
            ds,
            road_graph_name,
        )
        add_additional_property(
            road_object_iri,
            "missing_qrt_road",
            True,
            ds,
            road_graph_name,
        )

        # Road name lifecycle stage
        bnode = BNode(f"{road_id}-lifecycle-stage")
        ds.add((road_iri, LC.hasLifecycleStage, bnode, road_graph_name))
        ds.add((bnode, SDO.additionalType, lifecycle_stage_current, road_graph_name))

        # Name template
        ds.add(
            (
                road_iri,
                CN.nameTemplate,
                Literal(
                    f"{{RNPT.roadGivenName}} {{RNPT.roadType}} {{RNPT.roadSuffix}}"
                ),
                road_graph_name,
            )
        )

        # Road given name
        road_name_value = road_row[ROAD_NAME]
        if road_name_value:
            bnode = BNode(f"{road_id}-road-name")
            ds.add((road_iri, SDO.hasPart, bnode, road_graph_name))
            ds.add(
                (
                    bnode,
                    SDO.additionalType,
                    RNPT.roadGivenName,
                    road_graph_name,
                )
            )
            ds.add((bnode, SDO.value, Literal(road_name_value), road_graph_name))

        # Road type
        road_type_value = road_row[ROAD_NAME_TYPE_CODE]
        if road_type_value and road_type_value != "XXX":
            bnode = BNode(f"{road_id}-road-type")
            ds.add((road_iri, SDO.hasPart, bnode, road_graph_name))
            ds.add(
                (
                    bnode,
                    SDO.additionalType,
                    RNPT.roadType,
                    road_graph_name,
                )
            )

            concept = get_concept_from_vocab(
                SKOS.altLabel,
                Literal(road_type_value, lang="en"),
                road_type_concept_scheme,
                road_name_types_graph,
            )
            if concept is None:
                concept = get_concept_from_vocab(
                    SKOS.altLabel,
                    Literal(road_type_value, lang="en-AU"),
                    road_type_concept_scheme,
                    road_name_types_graph,
                )
            if not concept:
                raise Exception(
                    f"Concept IRI not found for road type value '{road_type_value}'"
                )
            ds.add((bnode, SDO.value, concept, road_graph_name))

        # Road suffix
        road_suffix_value = road_row[ROAD_NAME_SUFFIX_CODE]
        if road_suffix_value:
            bnode = BNode(f"{road_id}-road-suffix")
            ds.add((road_iri, SDO.hasPart, bnode, road_graph_name))
            ds.add(
                (
                    bnode,
                    SDO.additionalType,
                    RNPT.roadSuffix,
                    road_graph_name,
                )
            )
            concept = get_concept_from_vocab(
                SKOS.altLabel,
                Literal(road_suffix_value, lang="en"),
                road_suffix_concept_scheme,
                road_name_suffixes_graph,
            )
            if not concept:
                raise Exception(
                    f"Concept IRI not found for road suffix value '{road_suffix_value}'"
                )
            ds.add((bnode, SDO.value, concept, road_graph_name))

        # Road name
        ds.add((road_iri, RDF.type, ROADS.RoadName, road_graph_name))
        ds.add((road_iri, RDF.type, CN.CompoundName, road_graph_name))
        ds.add((road_iri, CN.isNameFor, road_object_iri, road_graph_name))
        ds.add((road_iri, SDO.name, Literal(road_row[ROAD_NAME_FULL]), road_graph_name))

    output_dir = Path(output_dir_name)
    filename = Path(dataset_name + "-" + str(job_id) + ".nq")
    serialize(output_dir, str(filename), ds)


def main():
    start_time = time.time()

    road_name_types_graph = get_vocab_graph([ROAD_NAME_TYPES_VOCAB_URL])
    road_name_suffixes_graph = get_vocab_graph([ROAD_NAME_SUFFIXES_VOCAB_URL])
    print(
        f"Remotely fetched {len(road_name_types_graph)} statements for road_name_types_graph"
    )
    print(
        f"Remotely fetched {len(road_name_suffixes_graph)} statements for road_name_suffixes_graph"
    )

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
                        r.road_id,
                        r.road_name,
                        r.road_name_type_code,
                        r.road_name_suffix_code,
                        r.qrt_road_name_basic as road_name_full
                    FROM "lalfpdba.lf_road" r
                    JOIN "lalfpdba.lf_address" a on r.road_id = a.road_id 
                    WHERE r.qrt_road_id is null and a.addr_status_code != 'H'
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
                            road_name_types_graph,
                            road_name_suffixes_graph,
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
