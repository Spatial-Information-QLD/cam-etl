import time
import concurrent.futures
from textwrap import dedent
from pathlib import Path

from psycopg import Cursor
from rdflib import Graph, URIRef, RDF, SDO, Literal, SKOS, BNode, TIME, XSD, PROV
from rdflib.namespace import GEO

from cam.etl import (
    add_additional_property,
    get_vocab_graph,
    get_db_connection,
    worker_wrap,
    serialize,
)
from cam.etl.pndb import vocab_mapping
from cam.etl.namespaces import GN, sir_id_datatype, CN, LC, GNPT, GN_STATUS
from cam.etl.types import Row
from cam.etl.settings import settings


dataset = "pndb"
output_dir_name = "pndb-rdf"

INDIGENOUS_GROUP_IRI = URIRef(
    "https://linked.data.gov.au/def/naming-authority/indigenous-group"
)
PLACE_NAMES_ACT_IRI = URIRef(
    "https://linked.data.gov.au/def/naming-authority/qld-pn-act-1994"
)
GO_CATEGORIES_URL = "https://cdn.jsdelivr.net/gh/geological-survey-of-queensland/vocabularies@b07763c87f2f872133197e6fb0eb911de85879c6/vocabularies-qsi/go-categories.ttl"

OBJECTID = "objectid"
REFERENCE_NUMBER = "reference_number"
HISTORIC_REFERENCE_NUMBER = "historic_reference_number"
PLACE_NAME = "place_name"
TYPE_RESOLVED = "type_resolved"
STATUS = "status"
CURRENCY = "currency"
GAZETTED_DATE = "gazetted_date"
GAZETTE_PAGE = "gazette_page"
LONGITUDE_DD = "longitude_dd"
LATITUDE_DD = "latitude_dd"
COMMENTS = "comments"
ORIGIN = "origin"
HISTORY = "history"
LINKS = "links"
LANGUAGE = "language"
PRONUNCIATION = "pronunciation"
SOURCE = "source"
DATE_ADDED = "date_added"
STATUS = "status"
PREFERRED = "preferred"
CREATED_USER = "created_user"
CREATED_DATE = "created_date"
INTERNAL_COMMENTS = "internal_comments"
PLACE_CURRENCY = "place_currency"
TAG = "tag"
TAG_RESOLVED = "tag_resolved"


def get_iri(reference_number: str):
    return URIRef(f"https://linked.data.gov.au/dataset/qld-addr/go/{reference_number}")


def get_label_iri(reference_number: str):
    return URIRef(f"https://linked.data.gov.au/dataset/qld-addr/gn/{reference_number}")


def get_indigenous_label_iri(reference_number: str, objectid: str):
    return URIRef(
        f"https://linked.data.gov.au/dataset/qld-addr/gn/{reference_number}-{objectid}"
    )


def transform_row():
    pass


def add_geographical_object(row: Row, graph: Graph, vocab_graph: Graph) -> None:
    iri = get_iri(row[REFERENCE_NUMBER])
    graph.add((iri, RDF.type, GN.GeographicalObject))

    # sdo:identifier
    graph.add(
        (
            iri,
            SDO.identifier,
            Literal(row[REFERENCE_NUMBER], datatype=sir_id_datatype),
        )
    )

    # sdo:additionalType
    value = vocab_graph.value(
        predicate=SKOS.prefLabel, object=Literal(row[TYPE_RESOLVED], lang="en")
    )
    if value is None:
        value = vocab_mapping.get(row[TYPE_RESOLVED])
    if value is None:
        raise Exception(
            f"No geographical object category concept matched for value {row[TYPE_RESOLVED]}."
        )
    graph.add((iri, SDO.additionalType, value))

    # geo:hasGeometry
    bnode_geometry = BNode(f"go-geo-hasGeometry-{row[REFERENCE_NUMBER]}")
    graph.add((iri, GEO.hasGeometry, bnode_geometry))
    graph.add(
        (
            bnode_geometry,
            GEO.asWKT,
            Literal(
                f"POINT ({row[LONGITUDE_DD]} {row[LATITUDE_DD]})",
                datatype=GEO.wktLiteral,
            ),
        )
    )


def add_lifecycle_stage(
    focus_node: URIRef | BNode,
    bnode_id: str,
    row: Row,
    graph: Graph,
    vocab_graph: Graph,
) -> None:
    bnode = BNode(bnode_id)
    graph.add((focus_node, LC.hasLifecycleStage, bnode))
    bnode_has_beginning = BNode(bnode_id + "-lifecycle-stage-has-beginning")
    graph.add((bnode, TIME.hasBeginning, bnode_has_beginning))
    if row.get(GAZETTED_DATE):
        graph.add(
            (
                bnode_has_beginning,
                TIME.inXSDDate,
                Literal(row[GAZETTED_DATE], datatype=XSD.date),
            )
        )

    reference_number = row[REFERENCE_NUMBER]
    status = row.get(STATUS)
    currency = row.get(CURRENCY)
    if status and currency:
        if status not in ("Y", "N"):
            raise ValueError(
                f"Unexpected value {status} for status in row with reference number {reference_number}"
            )
        if currency not in ("Y", "N"):
            raise ValueError(
                f"Unexpected value {currency} for currency in row with reference number {reference_number}"
            )

        # TODO: review with Michael
        match (status, currency):
            case ("Y", "Y"):
                graph.add((bnode, SDO.additionalType, GN_STATUS.gazetted))
            case ("N", "Y"):
                graph.add((bnode, SDO.additionalType, GN_STATUS.informal))
            case ("Y", "N"):
                graph.add((bnode, SDO.additionalType, GN_STATUS.retired))
            case ("N", "N"):
                graph.add((bnode, SDO.additionalType, GN_STATUS.historical))
            case _:
                raise ValueError(
                    f"Unmatched authority combination with status {status} and currency {currency}"
                )


def add_authority(
    focus_node: URIRef | BNode, row: Row, graph: Graph, vocab_graph: Graph
) -> None:
    reference_number = row[REFERENCE_NUMBER]
    status = row[STATUS]
    if status not in ("Y", "N"):
        raise ValueError(
            f"Unexpected value {status} for status in row with reference number {reference_number}"
        )
    currency = row[CURRENCY]
    if currency not in ("Y", "N"):
        raise ValueError(
            f"Unexpected value {currency} for currency in row with reference number {reference_number}"
        )

    match (status, currency):
        case ("Y", "Y") | ("Y", "N") | ("N", "N"):
            graph.add((focus_node, CN.hasAuthority, PLACE_NAMES_ACT_IRI))
        case ("N", "Y"):
            pass
        case _:
            raise ValueError(
                f"Unmatched authority combination with status {status} and currency {currency}"
            )


def add_geographical_name(row: Row, graph: Graph, vocab_graph: Graph) -> None:
    iri = get_iri(row[REFERENCE_NUMBER])
    label_iri = get_label_iri(row[REFERENCE_NUMBER])

    # Geographical Name
    graph.add((iri, SDO.name, label_iri))
    graph.add((label_iri, RDF.type, CN.CompoundName))
    graph.add((label_iri, RDF.type, GN.GeographicalName))
    graph.add((label_iri, CN.isNameFor, iri))
    graph.add((label_iri, SDO.name, Literal(row[PLACE_NAME])))

    # Lifecycle stage
    add_lifecycle_stage(
        label_iri,
        f"gn-lifecycle-stage-{row[REFERENCE_NUMBER]}",
        row,
        graph,
        vocab_graph,
    )

    # Name template
    graph.add(
        (
            label_iri,
            CN.nameTemplate,
            Literal(
                f"{{GNPT.geographicalPrefix}} {{GNPT.geographicalGivenName}} {{GNPT.geographicalSuffix}}"
            ),
        )
    )

    # Given Name Part
    bnode_given_name = BNode(f"gn-given-name-{row[REFERENCE_NUMBER]}")
    graph.add((label_iri, SDO.hasPart, bnode_given_name))
    graph.add((bnode_given_name, SDO.value, Literal(row[PLACE_NAME], lang="en")))
    graph.add((bnode_given_name, SDO.additionalType, GNPT.geographicalGivenName))

    # Authority
    add_authority(label_iri, row, graph, vocab_graph)

    # Add history note
    history_note = ""
    if origin := row[ORIGIN]:
        history_note += origin + "\n\n"
    if history := row[HISTORY]:
        history_note += history + "\n\n"
    if comments := row[COMMENTS]:
        history_note += comments + "\n\n"
    history_note = history_note.strip()
    if history_note:
        graph.add((label_iri, SKOS.historyNote, Literal(history_note, lang="en")))

    # Property values
    if status := row[STATUS]:
        add_additional_property(label_iri, STATUS, status, graph)
    if currency := row[CURRENCY]:
        add_additional_property(label_iri, CURRENCY, currency, graph)
    if gazette_page := row[GAZETTE_PAGE]:
        add_additional_property(label_iri, GAZETTE_PAGE, gazette_page, graph)
    if links := row[LINKS]:
        add_additional_property(label_iri, LINKS, links, graph)
    if pronunciation := row[PRONUNCIATION]:
        add_additional_property(label_iri, PRONUNCIATION, pronunciation, graph)
    if internal_comments := row[INTERNAL_COMMENTS]:
        add_additional_property(label_iri, INTERNAL_COMMENTS, internal_comments, graph)
    if place_currency := row[PLACE_CURRENCY]:
        add_additional_property(label_iri, PLACE_CURRENCY, place_currency, graph)


def add_indigenous_name(row: Row, graph: Graph, vocab_graph: Graph) -> None:
    iri = get_iri(row[REFERENCE_NUMBER])
    label_iri = get_indigenous_label_iri(row[REFERENCE_NUMBER], row[OBJECTID])

    # Geographical name
    graph.add((iri, SDO.name, label_iri))
    graph.add((label_iri, RDF.type, CN.CompoundName))
    graph.add((label_iri, RDF.type, GN.GeographicalName))
    graph.add((label_iri, CN.isNameFor, iri))
    graph.add((label_iri, SDO.name, Literal(row[PLACE_NAME])))

    # Lifecycle
    add_lifecycle_stage(
        label_iri,
        f"gn-lifecycle-stage-{row[REFERENCE_NUMBER]}-{hash(row[PLACE_NAME])}",
        row,
        graph,
        vocab_graph,
    )

    # Given Name Part
    bnode_given_name = BNode(
        f"gn-given-name-{row[REFERENCE_NUMBER]}-{hash(row[PLACE_NAME])}"
    )
    graph.add((label_iri, SDO.hasPart, bnode_given_name))
    # TODO: add indigenous language code datatype.
    #       we currently don't have this information in the data.
    graph.add((bnode_given_name, SDO.value, Literal(row[PLACE_NAME], lang="aus")))
    graph.add((bnode_given_name, SDO.additionalType, GNPT.geographicalGivenName))

    # Authority
    graph.add((label_iri, CN.hasAuthority, INDIGENOUS_GROUP_IRI))

    # Add additional properties
    if language := row[LANGUAGE]:
        add_additional_property(iri, LANGUAGE, language, graph)
    if pronunciation := row[PRONUNCIATION]:
        add_additional_property(iri, PRONUNCIATION, pronunciation, graph)
    if source := row[SOURCE]:
        add_additional_property(iri, SOURCE, source, graph)
    if date_added := row[DATE_ADDED]:
        add_additional_property(iri, DATE_ADDED, date_added, graph)
    if status := row[STATUS]:
        add_additional_property(iri, STATUS, status, graph)
    if preferred := row[PREFERRED]:
        add_additional_property(iri, PREFERRED, preferred, graph)
    if created_user := row[CREATED_USER]:
        add_additional_property(iri, CREATED_USER, created_user, graph)
    if created_date := row[CREATED_DATE]:
        add_additional_property(iri, CREATED_DATE, created_date, graph)
    if internal_comments := row[INTERNAL_COMMENTS]:
        add_additional_property(iri, INTERNAL_COMMENTS, internal_comments, graph)


def add_tag(row: Row, graph: Graph, vocab_graph: Graph) -> None:
    label_iri = get_label_iri(row[REFERENCE_NUMBER])
    if tag := row[TAG]:
        add_additional_property(label_iri, TAG, tag, graph)
    if tag_resolved := row[TAG_RESOLVED]:
        add_additional_property(label_iri, TAG_RESOLVED, tag_resolved, graph)
    if created_user := row[CREATED_USER]:
        add_additional_property(label_iri, CREATED_USER, created_user, graph)
    if created_date := row[CREATED_DATE]:
        add_additional_property(label_iri, CREATED_DATE, created_date, graph)


def get_historic_rows(reference_number: str, cursor: Cursor) -> list[Row]:
    cursor.execute(
        f"""
            SELECT
                h.historic_reference_number
            FROM
                "pndb.history" h
            WHERE
                h.reference_number = '{reference_number}'
        """
    )
    return cursor.fetchall()


def get_indigenous_rows(reference_number: str, cursor: Cursor) -> list[Row]:
    cursor.execute(
        f"""
            SELECT *
            FROM
                "pndb.indigenous_name" i
            WHERE
                i.reference_number = '{reference_number}'
        """
    )
    return cursor.fetchall()


def get_tag_rows(reference_number: str, cursor: Cursor) -> list[Row]:
    cursor.execute(
        f"""
            SELECT *
            FROM
                "pndb.tags" t
            WHERE
                t.reference_number = '{reference_number}'
        """
    )
    return cursor.fetchall()


@worker_wrap
def worker(rows: list[Row], job_id: int, vocab_graph: Graph):
    graph = Graph(store="Oxigraph")

    with get_db_connection(
        host=settings.etl.db.host,
        port=settings.etl.db.port,
        dbname=settings.etl.db.name,
        user=settings.etl.db.user,
        password=settings.etl.db.password,
    ) as connection:

        with connection.cursor() as cursor:
            for row in rows:
                reference_number = row[REFERENCE_NUMBER]
                add_geographical_object(row, graph, vocab_graph)
                add_geographical_name(row, graph, vocab_graph)

                if historic_rows := get_historic_rows(reference_number, cursor):
                    for historic_row in historic_rows:
                        iri = get_iri(row[REFERENCE_NUMBER])
                        historic_iri = get_iri(historic_row[HISTORIC_REFERENCE_NUMBER])
                        graph.add((iri, PROV.wasDerivedFrom, historic_iri))

                if indigenous_rows := get_indigenous_rows(reference_number, cursor):
                    for indigenous_row in indigenous_rows:
                        add_indigenous_name(indigenous_row, graph, vocab_graph)

                if tag_rows := get_tag_rows(reference_number, cursor):
                    for tag_row in tag_rows:
                        add_tag(tag_row, graph, vocab_graph)

    output_dir = Path(output_dir_name)
    filename = Path(dataset + "-" + str(job_id) + ".nt")
    serialize(output_dir, filename, graph)


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

        with connection.cursor(name="main", scrollable=False) as cursor:
            cursor.itersize = settings.etl.batch_size
            cursor.execute(
                dedent(
                    """\
                    SELECT
                        pn.*
                    FROM
                        "pndb.place_name" pn
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
