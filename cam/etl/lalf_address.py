import uuid

from rdflib import URIRef

qld_addr_namespace = uuid.uuid5(
    uuid.NAMESPACE_URL, "https://linked.data.gov.au/dataset/qld-addr/address/"
)

addr_status_vocab_mapping = {
    "A": URIRef("https://linked.data.gov.au/def/address-status-type/alternative"),
    "P": URIRef("https://linked.data.gov.au/def/address-status-type/primary"),
    "U": URIRef("https://linked.data.gov.au/def/address-status-type/unknown"),
    "X": URIRef("https://linked.data.gov.au/def/address-status-type/unofficial"),
}

addr_level_type_vocab_mapping = {
    "L": URIRef("https://linked.data.gov.au/def/building-level-types/level"),
}


def get_address_uuid(addr_id: str) -> str:
    return uuid.uuid5(qld_addr_namespace, addr_id)


def get_address_iri(addr_id: str) -> URIRef:
    addr_uuid = get_address_uuid(addr_id)
    return URIRef(f"https://linked.data.gov.au/dataset/qld-addr/address/{addr_uuid}")
