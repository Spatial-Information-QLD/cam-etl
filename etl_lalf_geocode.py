import time
import concurrent.futures
from textwrap import dedent
from pathlib import Path

from rdflib import Dataset, Graph, URIRef, RDF, Literal, SDO, SKOS
from rdflib.namespace import GEO

from cam.etl import (
    add_additional_property,
    get_db_connection,
    get_vocab_graph,
    worker_wrap,
    serialize,
)
from cam.etl.lalf_address import get_address_iri
from cam.etl.lalf_geocode import vocab_mapping
from cam.etl.namespaces import ADDR
from cam.etl.types import Row
from cam.etl.settings import settings

dataset_name = "lalf_geocode"
output_dir_name = "lalf-rdf"
graph_name = URIRef("urn:qali:graph:addresses")

GEOCODE_TYPES_URL = "https://cdn.jsdelivr.net/gh/geological-survey-of-queensland/vocabularies@b07763c87f2f872133197e6fb0eb911de85879c6/vocabularies-qsi/addr-geocode-types.ttl"

ADDR_ID = "addr_id"
GEOCODE_ID = "geocode_id"
GEOCODE_STATUS_CODE = "geocode_status_code"
GEOCODE_TYPE_CODE = "geocode_type_code"
SITE_ID = "site_id"
SPDB_PID = "spdb_pid"
GEOCODE_CREATE_DATE = "geocode_create_date"
GEOCODE_DATA_SOURCE_DATE = "geocode_data_source_date"
SURVEY_POINT_ID = "survey_point_id"
PID = "pid"
PLAN_NO = "plan_no"
LOT_NO = "lot_no"
CENTROID_LAT = "centroid_lat"
CENTROID_LON = "centroid_lon"


def get_iri(geocode_id: str) -> URIRef:
    return URIRef(f"https://linked.data.gov.au/dataset/qld-addr/geocode/{geocode_id}")


@worker_wrap
def worker(rows: list[Row], job_id: int, vocab_graph: Graph):
    ds = Dataset(store="Oxigraph")

    for row in rows:
        addr_iri = get_address_iri(row[ADDR_ID])
        geocode_iri = get_iri(row[GEOCODE_ID])
        ds.add((addr_iri, ADDR.hasGeocode, geocode_iri, graph_name))
        ds.add((geocode_iri, RDF.type, GEO.Geometry, graph_name))

        # geocode type
        value = vocab_graph.value(
            predicate=SKOS.altLabel, object=Literal(row[GEOCODE_TYPE_CODE], lang="en")
        )
        if value is None:
            value = vocab_mapping.get(row[GEOCODE_TYPE_CODE])
        if value is None:
            raise Exception(
                f"No geocode type concept matched for value {row[GEOCODE_TYPE_CODE]}."
            )
        ds.add((geocode_iri, SDO.additionalType, value, graph_name))

        # geocode
        geom = Literal(
            f"POINT ({row[CENTROID_LON]} {row[CENTROID_LAT]})", datatype=GEO.wktLiteral
        )
        ds.add((geocode_iri, GEO.asWKT, geom, graph_name))

        # geocode_id
        add_additional_property(
            geocode_iri, GEOCODE_ID, row[GEOCODE_ID], ds, graph_name
        )

        # geocode_status_code
        add_additional_property(
            geocode_iri, GEOCODE_STATUS_CODE, row[GEOCODE_STATUS_CODE], ds, graph_name
        )

        # geocode_type_code
        add_additional_property(
            geocode_iri, GEOCODE_TYPE_CODE, row[GEOCODE_TYPE_CODE], ds, graph_name
        )

        # site_id
        add_additional_property(geocode_iri, SITE_ID, row[SITE_ID], ds, graph_name)

        # spdb_pid
        add_additional_property(geocode_iri, SPDB_PID, row[SPDB_PID], ds, graph_name)

        # geocode_create_date
        add_additional_property(
            geocode_iri, GEOCODE_CREATE_DATE, row[GEOCODE_CREATE_DATE], ds, graph_name
        )

        # geocode_data_source_date
        add_additional_property(
            geocode_iri,
            GEOCODE_DATA_SOURCE_DATE,
            row[GEOCODE_DATA_SOURCE_DATE],
            ds,
            graph_name,
        )

        # survey_point_id
        add_additional_property(
            geocode_iri, SURVEY_POINT_ID, row[SURVEY_POINT_ID], ds, graph_name
        )

        # pid
        add_additional_property(geocode_iri, PID, row[PID], ds, graph_name)

        # plan_no
        add_additional_property(geocode_iri, PLAN_NO, row[PLAN_NO], ds, graph_name)

        # lot_no
        add_additional_property(geocode_iri, LOT_NO, row[LOT_NO], ds, graph_name)

    output_dir = Path(output_dir_name)
    filename = Path(dataset_name + "-" + str(job_id) + ".nq")
    serialize(output_dir, str(filename), ds)


def main():
    start_time = time.time()

    vocab_graph = get_vocab_graph([GEOCODE_TYPES_URL])
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
                        a.addr_id,
                        g.geocode_id,
                        g.geocode_status_code,
                        g.geocode_type_code,
                        g.site_id,
                        g.spdb_pid,
                        g.geocode_create_date,
                        g.geocode_data_source_date,
                        sp.survey_point_id,
                        sp.pid,
                        sp.plan_no,
                        sp.lot_no,
                        sp.centroid_lat,
                        sp.centroid_lon
                    FROM "lalfpdba.lf_geocode" g
                        join "lalfpdba.sp_survey_point" sp on g.spdb_pid = sp.pid
                        join "lalfpdba.lf_address" a on g.site_id = a.site_id
                    WHERE
                        g.geocode_status_code != 'H'
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
