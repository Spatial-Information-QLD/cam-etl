"""
This script pulls the addresses from GraphDB and generates the compounded name label
for each address and writes it to an n-quads file.

Running this script takes just over 7 hours to complete.

Loading the data into GraphDB via server files takes around 5 minutes.
"""

import os
import shutil
import time
import logging
from pathlib import Path

from rich.progress import track
from rdflib import Graph, URIRef, Literal
from rdflib.namespace import RDFS
from pyoxigraph import Store, serialize

from cam.graphdb import sparql
from cam.compound_naming import get_compound_name_object, template_address_one_liner
from cam.graph import prefixes


def create_graph(path: str):
    p = Path(path)
    if p.exists():
        shutil.rmtree(p)
    p.mkdir(parents=True)

    graph = Graph(store="Oxigraph", identifier="urn:graph:address-labels")
    graph.open(path)

    for key, val in prefixes.items():
        graph.bind(key, val)

    return graph


def to_file(table_name: str, graph: Graph):
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)
    filename = Path(table_name + "-" + str(os.getpid()) + ".nq")

    store: Store = graph.store._inner
    quads = store.quads_for_pattern(None, None, None)
    serialize(quads, str(output_dir / filename), "application/n-quads")


def main() -> None:
    graphdb_url = "http://localhost:7200"
    repository_id = "addressing"

    query = """
        SELECT ?iri
        WHERE {
            ?iri a <https://w3id.org/profile/anz-address/Address> .
        }
    """

    results = sparql(graphdb_url, repository_id, query)
    iris = [row["iri"]["value"] for row in results["results"]["bindings"]]

    oxigraph_path = Path(f"oxigraph_data/address_labels")
    graph = create_graph(str(oxigraph_path))

    for iri in track(iris, description="Creating address labels..."):
        compound_name_object = get_compound_name_object(
            iri, f"{graphdb_url}/repositories/{repository_id}"
        )
        address = template_address_one_liner(compound_name_object)

        if address != "":
            graph.add((URIRef(iri), RDFS.label, Literal(address)))
        else:
            logging.warning(f"Could not template address for {iri}.")

    print("Writing to disk...")
    to_file("address_labels", graph)


if __name__ == "__main__":
    starttime = time.time()

    try:
        main()
    finally:
        endtime = time.time() - starttime
        print(f"Completed in {endtime:0.2f} seconds")
