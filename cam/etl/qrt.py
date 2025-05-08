from rdflib import URIRef


def get_road_name_iri(road_id: str):
    return URIRef(f"https://linked.data.gov.au/dataset/qld-addr/road-name/{road_id}")
