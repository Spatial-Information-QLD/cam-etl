import time
import concurrent.futures
from textwrap import dedent
from pathlib import Path

from rdflib import Dataset, Graph, URIRef, SDO, BNode

from cam.etl import (
    get_db_connection,
    get_vocab_graph,
    worker_wrap,
    serialize,
)
from cam.etl.lalf_address import get_address_iri, get_address_uuid
from cam.etl.lalf_place_name import get_property_name_iri
from cam.etl.namespaces import ADDR_PT
from cam.etl.types import Row
from cam.etl.settings import settings

dataset_name = "lalf_property_name_on_address"
output_dir_name = "lalf-rdf"
graph_name = URIRef("urn:qali:graph:addresses")

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
        property_name_iri = get_property_name_iri(prop_id)

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
