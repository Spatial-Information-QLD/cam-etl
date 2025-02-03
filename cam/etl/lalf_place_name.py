import uuid

from rdflib import URIRef


property_namespace = uuid.uuid5(
    uuid.NAMESPACE_URL, "https://linked.data.gov.au/dataset/qld-addr/property/"
)


def get_property_name_iri(prop_id: str) -> URIRef:
    prop_uuid = uuid.uuid5(property_namespace, prop_id)
    return URIRef(f"https://linked.data.gov.au/dataset/qld-addr/gn/{prop_uuid}")
