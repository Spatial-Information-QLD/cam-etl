"""
Pre-validate the vocabulary mappings to ensure that the PNDB place name types are mappable to the concepts
in the Geographical Object Category vocabulary before running the PNDB ETL.
"""

import time
from textwrap import dedent

from rdflib import SKOS, Literal

from cam.etl import get_vocab_graph, get_db_connection
from cam.etl.pndb import vocab_mapping
from cam.etl.settings import settings

GO_CATEGORIES_URL = "https://cdn.jsdelivr.net/gh/geological-survey-of-queensland/vocabularies@b07763c87f2f872133197e6fb0eb911de85879c6/vocabularies-qsi/go-categories.ttl"


def main():
    start_time = time.time()

    vocab_graph = get_vocab_graph([GO_CATEGORIES_URL])
    print(f"Remotely fetched {len(vocab_graph)} statements for vocab_graph")

    with get_db_connection(
        host=settings.etl.db.host,
        port=settings.etl.db.port,
        dbname=settings.etl.db.name,
        user=settings.etl.db.user,
        password=settings.etl.db.password,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                dedent(
                    """\
                    SELECT DISTINCT
                        pn.type_resolved
                    FROM
                        "pndb.place_name" pn
                    ORDER BY
                        pn.type_resolved
                    """
                )
            )
            result_rows = cursor.fetchall()
            rows = [row["type_resolved"] for row in result_rows]
            for row in rows:
                value = vocab_graph.value(
                    predicate=SKOS.prefLabel, object=Literal(row, lang="en")
                )
                if value is None:
                    value = vocab_mapping.get(row)

                if value is None:
                    raise Exception(
                        f"No geographical object category concept matched for value {row}."
                    )

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Total execution time: {execution_time:.2f} seconds")
    print("Pre-validation checks complete")


if __name__ == "__main__":
    main()
