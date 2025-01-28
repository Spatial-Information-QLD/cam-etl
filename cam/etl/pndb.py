from rdflib import URIRef


# TODO: complete the mappings for values that are None.
vocab_mapping = {
    "Bank - Marine": URIRef("https://linked.data.gov.au/def/go-categories/bank"),
    "Channel": URIRef("https://linked.data.gov.au/def/go-categories/sea-channel"),
    "Cove, Inlet": URIRef("https://linked.data.gov.au/def/go-categories/inlet"),
    "Dam wall": URIRef("https://linked.data.gov.au/def/go-categories/dam-wall"),
    # TODO: Land District, Pastoral District? Maybe we need a more general District concept?
    "District": URIRef("https://linked.data.gov.au/def/go-categories/unclassified"),
    "ignore - test record": URIRef(
        "https://linked.data.gov.au/def/go-categories/unclassified"
    ),
    "Island - feature appears absent": URIRef(
        "https://linked.data.gov.au/def/go-categories/island"
    ),
    "Island group": URIRef("https://linked.data.gov.au/def/go-categories/island-group"),
    "Locality Bounded": URIRef("https://linked.data.gov.au/def/go-categories/locality"),
    "Locality Unbounded": URIRef(
        "https://linked.data.gov.au/def/go-categories/neighbourhood"
    ),
    # TODO: Lots of marine-based terms, need a more general marine concept?
    "Marine": URIRef("https://linked.data.gov.au/def/go-categories/unclassified"),
    "Mountain - Feature no longer exists": URIRef(
        "https://linked.data.gov.au/def/go-categories/mountain"
    ),
    # TODO: The vocab has both national park and conservation park??
    "National Park,Resources Reserve,Conservation Park": URIRef(
        "https://linked.data.gov.au/def/go-categories/unclassified"
    ),
    # TODO: Pass? Aquatic passage?
    "Passage": URIRef("https://linked.data.gov.au/def/go-categories/unclassified"),
    "Pastoral district": URIRef(
        "https://linked.data.gov.au/def/go-categories/pastoral-district"
    ),
    "Peak - Feature no longer exists": URIRef(
        "https://linked.data.gov.au/def/go-categories/peak"
    ),
    "Plateau - Marine": URIRef(
        "https://linked.data.gov.au/def/go-categories/marine-plateau"
    ),
    "Population centre": URIRef(
        "https://linked.data.gov.au/def/go-categories/population-centre"
    ),
    "Population centre - feature appears absent": URIRef(
        "https://linked.data.gov.au/def/go-categories/population-centre"
    ),
    "Rail Station - Feature no longer exists": URIRef(
        "https://linked.data.gov.au/def/go-categories/rail-station"
    ),
    "Reef": URIRef("https://linked.data.gov.au/def/go-categories/coral-reef"),
    # TODO: The vocab has a bunch of specific kinds of reserves. Do we need a more general reserve concept?
    "Reserve": URIRef("https://linked.data.gov.au/def/go-categories/unclassified"),
    "Ridge - Marine": URIRef(
        "https://linked.data.gov.au/def/go-categories/marine-ridge"
    ),
    "Shelf - Marine": URIRef(
        "https://linked.data.gov.au/def/go-categories/marine-shelf"
    ),
    "Shoal": URIRef("https://linked.data.gov.au/def/go-categories/marine-shoal"),
    "Suburb": URIRef("https://linked.data.gov.au/def/go-categories/locality"),
    "Water tank": URIRef("https://linked.data.gov.au/def/go-categories/water-tank"),
    # TODO: gas well? oil well? oil gas well?
    "Well": URIRef("https://linked.data.gov.au/def/go-categories/unclassified"),
}


def get_geographical_name_iri(reference_number: str):
    return URIRef(f"https://linked.data.gov.au/dataset/qld-addr/gn/{reference_number}")
