import rich
import requests
from pydantic import BaseModel, Field
from rdflib import Graph
from rdflib.namespace import RDF, SKOS


icsm_placenames_url = "http://icsm.surroundaustralia.com/object?uri=https%3A//linked.data.gov.au/def/placenames-categories&_profile=dd&_mediatype=application/json"


class Row(BaseModel):
    uri: str
    pref_label: str = Field(alias="prefLabel")


if __name__ == "__main__":
    response = requests.get(icsm_placenames_url)
    response.raise_for_status()

    icsm_placenames = set()
    for item in response.json():
        icsm_placenames.add(item["prefLabel"])

    graph = Graph()
    graph.parse("pntypes.ttl")

    not_found = []
    concepts = graph.subjects(RDF.type, SKOS.Concept)
    for concept in concepts:
        label = graph.value(concept, SKOS.prefLabel)
        name = str(label).lower()
        if name not in icsm_placenames:
            not_found.append(name)

    rich.print("Not found:")
    rich.print(sorted(not_found))
