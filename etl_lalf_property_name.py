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
from cam.etl.lalf_address import get_address_iri, get_address_uuid
from cam.etl.lalf_parcel import get_parcel_iri
from cam.etl.lalf_place_name import get_property_name_iri, property_namespace
from cam.etl.namespaces import (
    ADDR_PT,
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

PROPERTY_NAME = "property_name"
LOT_NO = "lot"
PLAN_NO = "plan"
PROP_ID = "id"
ADDR_ID = "addr_id"


@worker_wrap
def worker(rows: list[Row], job_id: int, vocab_graph: Graph):
    ds = Dataset(store="Oxigraph")

    for row in rows:
        prop_id = row[PROP_ID]
        lot_no = row[LOT_NO] if row[LOT_NO] != "0" else "9999"
        plan_no = row[PLAN_NO]
        prop_uuid = uuid.uuid5(property_namespace, prop_id)

        # gn object
        parcel_iri = get_parcel_iri(lot_no, plan_no)
        property_name_iri = get_property_name_iri(prop_id)
        ds.add((parcel_iri, RDF.type, GN.GeographicalObject, graph_name))
        ds.add((parcel_iri, CN.hasName, property_name_iri, graph_name))

        # gn
        label = row[PROPERTY_NAME]
        ds.add((property_name_iri, RDF.type, CN.CompoundName, graph_name))
        ds.add((property_name_iri, RDF.type, GN.GeographicalName, graph_name))
        ds.add((property_name_iri, CN.isNameFor, parcel_iri, graph_name))
        ds.add(
            (
                property_name_iri,
                SDO.identifier,
                Literal(prop_id, datatype=property_datatype),
                graph_name,
            )
        )
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

        # Address part
        addr_id_uuid = get_address_uuid(row[ADDR_ID])
        addr_id = row[ADDR_ID]
        addr_iri = get_address_iri(addr_id)
        property_name_node = BNode(f"{addr_id_uuid}-property-name")
        ds.add((addr_iri, SDO.hasPart, property_name_node, graph_name))
        ds.add(
            (
                property_name_node,
                SDO.additionalType,
                ADDR_PT.propertyName,
                graph_name,
            )
        )
        ds.add((property_name_node, SDO.value, property_name_iri, graph_name))

        # lifecycle stage
        bnode_id = f"{addr_id_uuid}-{prop_uuid}-lifecycle"
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
                    SELECT pa.*, a.addr_id
                    FROM lalf_property_address_joined pa
                    JOIN "lalfpdba.lf_parcel" p on p.lot_no = pa.lot and p.plan_no = pa.plan
                    JOIN "lalfpdba.lf_site" s on s.parcel_id = p.parcel_id
                    JOIN "lalfpdba.lf_address" a on a.site_id = s.site_id
                    GROUP BY pa.property_name, pa.lot, pa.plan, pa.id, a.addr_id
                    limit 1
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
