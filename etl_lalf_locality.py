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

dataset = "lalf"
output_dir_name = "lalf-rdf"


@worker_wrap
def worker(rows: list[Row], job_id: int, vocab_graph: Graph):
    graph = Graph(store="Oxigraph")

    for row in rows:
        pass

    # output_dir = Path(output_dir_name)
    # filename = Path(dataset + "-" + str(job_id) + ".nt")
    # serialize(output_dir, filename, graph)


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
                        "lalfpdba.locality"
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
