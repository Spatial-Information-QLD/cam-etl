import uuid

from rdflib import URIRef


road_object_namespace = uuid.uuid5(
    uuid.NAMESPACE_URL, "https://linked.data.gov.au/dataset/qld-addr/road/"
)
road_name_namespace = uuid.uuid5(
    uuid.NAMESPACE_URL, "https://linked.data.gov.au/dataset/qld-addr/road-name/"
)


def get_road_object_iri(road_id: str):
    road_uuid = uuid.uuid5(road_object_namespace, str(road_id))
    return URIRef(f"https://linked.data.gov.au/dataset/qld-addr/road/{road_uuid}")


def get_road_name_iri(road_id: str):
    road_name_uuid = uuid.uuid5(road_name_namespace, str(road_id))
    return URIRef(
        f"https://linked.data.gov.au/dataset/qld-addr/road-name/{road_name_uuid}"
    )
