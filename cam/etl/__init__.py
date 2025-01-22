from pathlib import Path
from functools import wraps
from contextlib import contextmanager
from typing import Iterator

import psycopg
import psycopg.rows
import pyoxigraph
from rdflib import Graph, URIRef, BNode, SDO, Literal, SKOS


def serialize(output_dir: Path, filename: str, graph: Graph):
    output_dir.mkdir(exist_ok=True)
    store: pyoxigraph.Store = graph.store._inner
    quads = store.quads_for_pattern(None, None, None)
    triples = (
        pyoxigraph.Triple(quad.subject, quad.predicate, quad.object) for quad in quads
    )
    pyoxigraph.serialize(triples, str(output_dir / filename), "application/n-triples")


def worker_wrap(f):
    """
    Decorator to add some logging outputs when a worker starts and completes a job.
    """

    @wraps(f)
    def wrapper(*args):
        job_id = args[1]
        print(f"Job ID {job_id} starting")
        f(*args)
        print(f"Job ID {job_id} completed")

    return wrapper


@contextmanager
def get_db_connection(host: str, port: int, dbname: str, user: str, password: str) -> Iterator[psycopg.Connection]:
    """
    Get a database connection with a context manager.
    """
    connection: psycopg.Connection = psycopg.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
        row_factory=psycopg.rows.dict_row,
    )
    try:
        yield connection
    finally:
        connection.close()


def get_vocab_graph(urls: list[str]):
    """
    Get a vocab graph by fetching the web contents via a list of URLs.
    """
    graph = Graph()
    for url in urls:
        graph.parse(url)
    return graph


def get_concept_from_vocab(
    predicate: URIRef, value: Literal, concept_scheme: URIRef, graph: Graph
) -> URIRef | None:
    for concept in graph.subjects(SKOS.inScheme, concept_scheme):
        if (concept, predicate, value) in graph:
            return concept
    return None


def add_additional_property(
    focus_node: URIRef | BNode, property_key: str, property_value: str, graph: Graph
):
    """
    Create a schema.org sdo:PropertyValue object linked from the focus node with sdo:additionalProperty.
    """

    # Create a positive value that is compatible with oxigraph blank nodes based on the hash of the string identifier.
    safe_id = f"b{hash(str(focus_node) + property_key) & 0xFFFFFFFF:x}"
    bnode = BNode(safe_id)
    graph.add((focus_node, SDO.additionalProperty, bnode))
    graph.add((bnode, SDO.propertyID, Literal(property_key)))
    graph.add((bnode, SDO.value, Literal(property_value)))
