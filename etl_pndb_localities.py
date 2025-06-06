import time
import concurrent.futures
from textwrap import dedent
from pathlib import Path

from rdflib import (
    Dataset,
    Graph,
    URIRef,
    RDF,
    SDO,
    Literal,
    BNode,
    TIME,
    XSD,
)

from cam.etl import (
    add_additional_property,
    get_vocab_graph,
    get_db_connection,
    worker_wrap,
    serialize,
)
from cam.etl.pndb import get_geographical_name_iri
from cam.etl.namespaces import GN, CN, LC, GNPT, GN_STATUS, pndb_datatype
from cam.etl.types import Row
from cam.etl.settings import settings


dataset_name = "pndb"
output_dir_name = "pndb-rdf"
graph_name = URIRef("urn:qali:graph:geographical-names")

PLACE_NAMES_ACT_IRI = URIRef(
    "https://linked.data.gov.au/def/naming-authority/qld-pn-act-1994"
)
GO_CATEGORIES_URL = "https://cdn.jsdelivr.net/gh/icsm-au/icsm-vocabs@main/vocabs/GeographicalNames/go-categories.ttl"


C_REF_NO = "pndb.ref_no"
C_LALF_LOCALITY_CODE = "lalf.locality_code"
C_PLACE_NAME = "pndb.place_name"
C_GAZETTED_DATE = "pndb.gazetted_date"
C_HISTORY = "pndb.history"
C_COMMENTS = "pndb.comments"
C_STATUS = "pndb.status"
C_CURRENCY = "pndb.currency"
C_GAZETTE_PAGE = "pndb.gazette_page"
C_PLAN_NO = "pndb.plan_no"
C_LGA_NAME = "pndb.lga_name"
C_LALF_LOCALITY_CODE = "lalf.locality_code"
C_LALF_LOCALITY_NAME = "lalf.locality_name"
C_LALF_LOCALITY_TYPE = "lalf.locality_type"
C_LALF_LA_CODE = "lalf.la_code"
C_LALF_STATE = "lalf.state"
C_LALF_CURRENCY_STATUS = "lalf.currency_status"


def get_geographical_object_iri(ref_no: str):
    return URIRef(f"https://linked.data.gov.au/dataset/qld-addr/go/{ref_no}")


def add_geographical_object(row: Row, ds: Dataset, vocab_graph: Graph):
    ref_no = row[C_REF_NO]
    iri = get_geographical_object_iri(ref_no)

    ds.add((iri, RDF.type, GN.GeographicalObject, graph_name))

    # geographic object category
    ds.add(
        (
            iri,
            SDO.additionalType,
            URIRef("https://linked.data.gov.au/def/go-categories/locality"),
            graph_name,
        )
    )

    # sdo:identifier
    ds.add(
        (
            iri,
            SDO.identifier,
            Literal(ref_no, datatype=pndb_datatype),
            graph_name,
        )
    )


def add_lifecycle_stage(
    focus_node: URIRef | BNode,
    bnode_id: str,
    row: Row,
    ds: Dataset,
    vocab_graph: Graph,
) -> None:
    bnode = BNode(bnode_id)
    ds.add((focus_node, LC.hasLifecycleStage, bnode, graph_name))
    bnode_has_beginning = BNode(bnode_id + "-lifecycle-stage-has-beginning")
    ds.add((bnode, TIME.hasBeginning, bnode_has_beginning, graph_name))
    ds.add(
        (
            bnode_has_beginning,
            TIME.inXSDDate,
            Literal(row[C_GAZETTED_DATE], datatype=XSD.date),
            graph_name,
        )
    )

    # All status and currency are Y Y.
    ds.add((bnode, SDO.additionalType, GN_STATUS.gazetted, graph_name))


def add_geographical_name(row: Row, ds: Dataset, vocab_graph: Graph):
    ref_no = row[C_REF_NO]
    place_name = row[C_PLACE_NAME]
    iri = get_geographical_name_iri(ref_no)
    object_iri = get_geographical_object_iri(ref_no)

    # Geographical Name
    ds.add((iri, RDF.type, CN.CompoundName, graph_name))
    ds.add((iri, RDF.type, GN.GeographicalName, graph_name))
    ds.add((object_iri, CN.hasName, iri, graph_name))
    ds.add((iri, CN.isNameFor, object_iri, graph_name))

    # Name
    ds.add((iri, SDO.name, Literal(place_name), graph_name))

    # Lifecycle stage
    add_lifecycle_stage(
        iri,
        f"gn-lifecycle-stage-{ref_no}",
        row,
        ds,
        vocab_graph,
    )

    # Name template
    ds.add(
        (
            iri,
            CN.nameTemplate,
            Literal(
                f"{{GNPT.geographicalPrefix}} {{GNPT.geographicalGivenName}} {{GNPT.geographicalSuffix}}"
            ),
            graph_name,
        )
    )

    # Given Name Part
    bnode_given_name = BNode(f"gn-given-name-{ref_no}")
    ds.add((iri, SDO.hasPart, bnode_given_name, graph_name))
    ds.add((bnode_given_name, SDO.value, Literal(place_name, lang="en"), graph_name))
    ds.add(
        (bnode_given_name, SDO.additionalType, GNPT.geographicalGivenName, graph_name)
    )

    # Authority
    ds.add((iri, CN.hasAuthority, PLACE_NAMES_ACT_IRI, graph_name))

    # Additional Attributes
    if history := row[C_HISTORY]:
        add_additional_property(iri, C_HISTORY, history, ds, graph_name)
    if comments := row[C_COMMENTS]:
        add_additional_property(iri, C_COMMENTS, comments, ds, graph_name)
    if status := row[C_STATUS]:
        add_additional_property(iri, C_STATUS, status, ds, graph_name)
    if currency := row[C_CURRENCY]:
        add_additional_property(iri, C_CURRENCY, currency, ds, graph_name)
    if gazette_page := row[C_GAZETTE_PAGE]:
        add_additional_property(iri, C_GAZETTE_PAGE, gazette_page, ds, graph_name)
    if plan_no := row[C_PLAN_NO]:
        add_additional_property(iri, C_PLAN_NO, plan_no, ds, graph_name)
    if lga_name := row[C_LGA_NAME]:
        add_additional_property(iri, C_LGA_NAME, lga_name, ds, graph_name)

    # Additional Attributes PLS
    if lalf_locality_code := row[C_LALF_LOCALITY_CODE]:
        add_additional_property(
            iri, C_LALF_LOCALITY_CODE, lalf_locality_code, ds, graph_name
        )
    if lalf_locality_name := row[C_LALF_LOCALITY_NAME]:
        add_additional_property(
            iri, C_LALF_LOCALITY_NAME, lalf_locality_name, ds, graph_name
        )
    if lalf_locality_type := row[C_LALF_LOCALITY_TYPE]:
        add_additional_property(
            iri, C_LALF_LOCALITY_TYPE, lalf_locality_type, ds, graph_name
        )
    if lalf_la_code := row[C_LALF_LA_CODE]:
        add_additional_property(iri, C_LALF_LA_CODE, lalf_la_code, ds, graph_name)
    if lalf_state := row[C_LALF_STATE]:
        add_additional_property(iri, C_LALF_STATE, lalf_state, ds, graph_name)
    if lalf_currency_status := row[C_LALF_CURRENCY_STATUS]:
        add_additional_property(
            iri, C_LALF_CURRENCY_STATUS, lalf_currency_status, ds, graph_name
        )


def transform_row(row: Row, ds: Dataset, vocab_graph: Graph):
    add_geographical_object(row, ds, vocab_graph)
    add_geographical_name(row, ds, vocab_graph)


@worker_wrap
def worker(rows: list[Row], job_id: int, vocab_graph: Graph):
    ds = Dataset(store="Oxigraph")

    for row in rows:
        transform_row(row, ds, vocab_graph)

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
                        l."pndb.ref_no",
                        l."lalf.locality_code",
                        -- name
                        l."pndb.place_name",
                        l."pndb.gazetted_date",
                        -- additional attributes
                        l."pndb.history",
                        l."pndb.comments",
                        l."pndb.status",
                        l."pndb.currency",
                        l."pndb.gazette_page",
                        l."pndb.plan_no",
                        l."pndb.lga_name",
                        -- pls
                        l."lalf.locality_code",
                        l."lalf.locality_name",
                        l."lalf.locality_type",
                        l."lalf.la_code",
                        l."lalf.state",
                        l."lalf.currency_status"
                    FROM lalf_pndb_localities_joined l
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
