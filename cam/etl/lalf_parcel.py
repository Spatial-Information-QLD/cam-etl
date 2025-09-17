from rdflib import URIRef


def get_parcel_iri(lot: str, plan: str) -> URIRef:
    return URIRef(f"https://linked.data.gov.au/dataset/qld-addr/parcel/{lot}{plan}")
