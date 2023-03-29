from rdflib import Graph, URIRef
from rdflib.namespace import Namespace, DefinedNamespace, GEO, DCTERMS, SKOS, XSD


class ADDR(DefinedNamespace):
    _fail = True
    _underscore_num = True
    _NS = Namespace("https://w3id.org/profile/anz-address/")

    AddressableObject: URIRef
    Address: URIRef
    isAddressFor: URIRef
    hasGeocode: URIRef
    Geocode: URIRef


prefixes = {"addr": ADDR, "geo": GEO, "dcterms": DCTERMS, "skos": SKOS, "xsd": XSD}


def get_new_graph():
    graph = Graph()

    for key, val in prefixes.items():
        graph.bind(key, val)

    return graph
