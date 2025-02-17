import logging
from textwrap import dedent
from itertools import islice

import httpx
import meilisearch
from jinja2 import Template
from rdflib import Graph, Namespace, RDF, RDFS, SDO, URIRef

CHUNK_SIZE = 10_000
# TODO: move to env
SPARQL_ENDPOINT = "http://localhost:3030/qali/query"

ADDR = Namespace("https://linked.data.gov.au/def/addr/")
CN = Namespace("https://linked.data.gov.au/def/cn/")


def get_lot_and_plan(graph: Graph, parcel_iri: str) -> tuple[str, str]:
    identifiers = graph.objects(parcel_iri, SDO.identifier)
    lot = None
    plan = None
    lot_uri = URIRef("https://linked.data.gov.au/dataset/qld-addr/datatype/lot")
    plan_uri = URIRef("https://linked.data.gov.au/dataset/qld-addr/datatype/plan")
    for identifier in identifiers:
        if identifier.datatype == lot_uri:
            lot = identifier
        elif identifier.datatype == plan_uri:
            plan = identifier
    return lot, plan


def get_query(address_iris: list[str]) -> str:
    return dedent(
        Template(
            """
            PREFIX 	cn: <https://linked.data.gov.au/def/cn/>
            PREFIX  rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX  text: <http://jena.apache.org/text#>
            PREFIX  addr: <https://linked.data.gov.au/def/addr/>
            PREFIX  prov: <http://www.w3.org/ns/prov#>
            PREFIX  sdo:  <https://schema.org/>

            CONSTRUCT {
            ?iri a addr:Address ;
                addr:hasStatus ?status ;
                cn:isNameFor ?obj ;
                sdo:identifier ?identifier ;
                rdfs:label ?label .
            
            ?obj a addr:AddressableObject ;
                rdfs:label ?objLabel ;
                cn:hasName ?name ;
                sdo:additionalType ?objType ;
                sdo:identifier ?objIdentifier .
            }
            FROM <urn:ladb:graph:addresses>
            where {
                VALUES ?iri {
                    {% for address_iri in address_iris %}
                    <{{ address_iri }}>
                    {% endfor %}
                }
            
                ?iri a addr:Address ;
                    addr:hasStatus ?status ;
                    cn:isNameFor ?obj ;
                    sdo:identifier ?identifier ;
                    rdfs:label ?label .
                
                ?obj rdfs:label ?objLabel ;
                    cn:hasName ?name ;
                        sdo:additionalType ?objType ;
                        sdo:identifier ?objIdentifier .
            }
            """
        ).render(address_iris=address_iris)
    )


def main():
    # TODO: move key to env
    client = meilisearch.Client(
        "http://localhost:7700",
    )
    index = client.index("addresses")

    with open("data_address_iris.nt", "r", encoding="utf-8") as file:

        with httpx.Client(timeout=None) as client:

            while True:
                # Read chunk_size lines
                lines = list(islice(file, CHUNK_SIZE))

                # If no more lines to read, break the loop
                if not lines:
                    break

                # Process the chunk of lines
                data = "\n".join(lines)
                iri_graph = Graph().parse(data=data, format="nt")
                address_iris = iri_graph.subjects(RDF.type, ADDR.Address)

                query = get_query(address_iris=address_iris)
                response = client.post(
                    SPARQL_ENDPOINT,
                    data=query,
                    headers={
                        "Content-Type": "application/sparql-query",
                        "Accept": "application/n-triples",
                    },
                )

                graph = Graph().parse(data=response.text, format="nt")
                documents = []
                for iri in graph.subjects(RDF.type, ADDR.Address):
                    label = graph.value(iri, RDFS.label)
                    if label is None:
                        raise ValueError(f"Label is None for {iri}")
                    status = graph.value(iri, ADDR.hasStatus)
                    if status is None:
                        raise ValueError(f"Status is None for {iri}")
                    parcel_iri = graph.value(iri, CN.isNameFor)
                    if parcel_iri is None:
                        raise ValueError(f"Parcel IRI is None for {iri}")
                    identifier = graph.value(iri, SDO.identifier)
                    if identifier is None:
                        raise ValueError(f"Identifier is None for {iri}")
                    lot, plan = get_lot_and_plan(graph, parcel_iri)
                    if lot is None:
                        lot = ""
                        logging.warning(f"Lot is None for {iri}")
                    if plan is None:
                        plan = ""
                        logging.warning(f"Plan is None for {iri}")

                    documents.append(
                        {
                            "id": identifier,
                            "iri": iri,
                            "label": label,
                            "status": status,
                            "parcel": parcel_iri,
                            "lot": lot,
                            "plan": plan,
                        },
                    )

                index.add_documents(documents)


if __name__ == "__main__":
    main()
