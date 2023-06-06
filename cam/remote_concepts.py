import requests
from rdflib import URIRef
from jinja2 import Template


def get_remote_concepts(
    sparql_endpoint: str, concept_scheme_iri: str
) -> dict[str, str]:
    query = Template(
        """
        PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        PREFIX dcterms: <http://purl.org/dc/terms/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        SELECT * WHERE {
        ?iri rdfs:isDefinedBy <{{ concept_scheme_iri }}> ;
            dcterms:identifier ?token .
        FILTER(datatype(?token) = xsd:token)
        }
    """
    ).render(concept_scheme_iri=concept_scheme_iri)
    response = requests.post(sparql_endpoint, data=query)

    if response.status_code != 200:
        raise RuntimeError(
            f"Failed to fetch concepts from {sparql_endpoint} for {concept_scheme_iri}."
        )

    concepts = {}

    rows = response.json()["results"]["bindings"]
    for row in rows:
        concepts.update({row["token"]["value"]: URIRef(row["iri"]["value"])})

    return concepts
