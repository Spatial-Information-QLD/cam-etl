import uuid
import time
import concurrent.futures
from textwrap import dedent
from pathlib import Path

from rdflib import Dataset, Graph, URIRef, RDF, Literal, SDO, BNode

from cam.etl import (
    get_db_connection,
    get_vocab_graph,
    worker_wrap,
    serialize,
)
from cam.etl.lalf_place_name import (
    get_property_name_iri,
    get_property_object_iri,
    property_namespace,
)
from cam.etl.namespaces import (
    GN,
    CN,
    LC,
    property_datatype,
    GNPT,
    lifecycle_stage_current,
)
from cam.etl.types import Row
from cam.etl.settings import settings

dataset_name = "lalf_place_name"
output_dir_name = "lalf-rdf"
graph_name = URIRef("urn:qali:graph:geographical-names")
property_category = URIRef("https://linked.data.gov.au/def/go-categories/property")

PROPERTY_NAME = "property_name"
PROP_ID = "id"


@worker_wrap
def worker(rows: list[Row], job_id: int, vocab_graph: Graph):
    ds = Dataset(store="Oxigraph")

    for row in rows:
        prop_id = row[PROP_ID]
        prop_uuid = uuid.uuid5(property_namespace, prop_id)

        # gn object
        property_object_iri = get_property_object_iri(prop_id)
        property_name_iri = get_property_name_iri(prop_id)
        ds.add((property_object_iri, RDF.type, GN.GeographicalObject, graph_name))
        ds.add((property_object_iri, CN.hasName, property_name_iri, graph_name))
        ds.add(
            (
                property_object_iri,
                SDO.identifier,
                Literal(prop_id, datatype=property_datatype),
                graph_name,
            )
        )
        ds.add((property_object_iri, SDO.additionalType, property_category, graph_name))

        # gn
        label = row[PROPERTY_NAME]
        ds.add((property_name_iri, RDF.type, CN.CompoundName, graph_name))
        ds.add((property_name_iri, RDF.type, GN.GeographicalName, graph_name))
        ds.add((property_name_iri, CN.isNameFor, property_object_iri, graph_name))
        ds.add((property_name_iri, SDO.name, Literal(label), graph_name))

        # gn - given name
        given_name_node = BNode(f"{prop_uuid}-given-name")
        ds.add((property_name_iri, SDO.hasPart, given_name_node, graph_name))
        ds.add(
            (
                given_name_node,
                SDO.additionalType,
                GNPT.geographicalGivenName,
                graph_name,
            )
        )
        ds.add((given_name_node, SDO.value, Literal(label, lang="en"), graph_name))

        # lifecycle stage
        bnode_id = f"{prop_uuid}-lifecycle"
        bnode = BNode(bnode_id)
        ds.add((property_name_iri, LC.hasLifecycleStage, bnode, graph_name))
        ds.add((bnode, SDO.additionalType, lifecycle_stage_current, graph_name))

    output_dir = Path(output_dir_name)
    filename = Path(dataset_name + "-" + str(job_id) + ".nq")
    serialize(output_dir, str(filename), ds)


def main():
    start_time = time.time()

    vocab_graph = get_vocab_graph([])
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
                    SELECT DISTINCT pn.pl_name_id AS id, pn.pl_name AS property_name
                    FROM lalf_place_names_joined_to_lalf_addr_id pn
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
