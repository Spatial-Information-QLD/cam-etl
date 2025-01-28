import time
import concurrent.futures
from textwrap import dedent
from pathlib import Path

from rdflib import Dataset, Graph, URIRef, RDF, RDFS, Literal, SDO

from cam.etl import (
    add_additional_property,
    get_db_connection,
    get_vocab_graph,
    worker_wrap,
    serialize,
)
from cam.etl.lalf_parcel import get_parcel_iri
from cam.etl.namespaces import ADDR, lot_datatype, plan_datatype
from cam.etl.types import Row
from cam.etl.settings import settings

dataset_name = "lalf_parcel"
output_dir_name = "lalf-rdf"
graph_name = URIRef("urn:ladb:graph:addresses")

# TODO: ensure this is added to go-categories
parcel_type = URIRef("https://linked.data.gov.au/def/go-categories/parcel")

PARCEL_ID = "parcel_id"
PLAN_NO = "plan_no"
LOT_NO = "lot_no"
PARCEL_STATUS_CODE = "parcel_status_code"
PARCEL_CREATE_DATE = "parcel_create_date"
PARCEL_ORG_SOURCE_CODE = "parcel_org_source_code"
PARCEL_DATA_SOURCE_CODE = "parcel_data_source_code"
PARCEL_DATA_SOURCE_DATE = "parcel_data_source_date"


@worker_wrap
def worker(rows: list[Row], job_id: int, vocab_graph: Graph):
    ds = Dataset(store="Oxigraph")

    for row in rows:
        parcel_iri = get_parcel_iri(row[LOT_NO], row[PLAN_NO])
        ds.add((parcel_iri, RDF.type, ADDR.AddressableObject, graph_name))
        ds.add((parcel_iri, SDO.additionalType, parcel_type, graph_name))

        # lot and plan
        ds.add(
            (
                parcel_iri,
                RDFS.label,
                Literal(f"{row[LOT_NO]}{row[PLAN_NO]}"),
                graph_name,
            )
        )
        if lot_no := row[LOT_NO]:
            ds.add(
                (
                    parcel_iri,
                    SDO.identifier,
                    Literal(lot_no, datatype=lot_datatype),
                    graph_name,
                )
            )
        if plan_no := row[PLAN_NO]:
            ds.add(
                (
                    parcel_iri,
                    SDO.identifier,
                    Literal(plan_no, datatype=plan_datatype),
                    graph_name,
                )
            )

        # parcel_id
        add_additional_property(parcel_iri, PARCEL_ID, row[PARCEL_ID], ds, graph_name)

        # parcel_status_code
        add_additional_property(
            parcel_iri, PARCEL_STATUS_CODE, row[PARCEL_STATUS_CODE], ds, graph_name
        )

        # parcel_create_date
        add_additional_property(
            parcel_iri, PARCEL_CREATE_DATE, row[PARCEL_CREATE_DATE], ds, graph_name
        )

        # parcel_data_source_date
        add_additional_property(
            parcel_iri,
            PARCEL_DATA_SOURCE_DATE,
            row[PARCEL_DATA_SOURCE_DATE],
            ds,
            graph_name,
        )

        # ignoring parcel_org_source_code and parcel_data_source_code because they only have one distinct value in each.

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
                    SELECT
                        *
                    FROM
                        "lalfpdba.lf_parcel" p
                    WHERE p.parcel_status_code != 'D'
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
