import shutil
from pathlib import Path

from rdflib import Graph, URIRef
from rdflib.namespace import (
    Namespace,
    DefinedNamespace,
    GEO,
    DCTERMS,
    SKOS,
    XSD,
    TIME,
    SDO,
)


class ADDR(DefinedNamespace):
    _fail = True
    _underscore_num = True
    _NS = Namespace("https://w3id.org/profile/anz-address/")

    AddressableObject: URIRef
    Address: URIRef
    Geocode: URIRef
    Locality: URIRef

    isAddressFor: URIRef
    hasAddress: URIRef
    hasGeocode: URIRef
    hasFeatureComponent: URIRef
    hasComponentType: URIRef
    hasValue: URIRef


class ADDRCMPType(DefinedNamespace):
    _fail = True
    _underscore_num = True
    _NS = Namespace("https://w3id.org/profile/anz-address/AnzAddressComponentTypes/")

    flatTypeCode: URIRef
    flatNumber: URIRef
    flatNumberSuffix: URIRef
    levelTypeCode: URIRef
    levelNumber: URIRef
    numberFirst: URIRef
    numberFirstSuffix: URIRef
    numberLast: URIRef
    numberLastSuffix: URIRef
    road: URIRef
    streetLocality: URIRef
    locality: URIRef
    stateOrTerritory: URIRef
    countryName: URIRef


class ASTISO(DefinedNamespace):
    _fail = True
    _underscore_num = True
    _NS = Namespace(
        "http://def.isotc211.org/iso19160/-1/2015/Address/code/AnzAddressStatusTypes/"
    )

    official: URIRef
    unknown: URIRef
    unofficial: URIRef


class ROADS(DefinedNamespace):
    _fail = True
    _underscore_num = True
    _NS = Namespace("https://linked.data.gov.au/def/roads/")

    RoadLabel: URIRef
    RoadObject: URIRef


class FL(DefinedNamespace):
    _fail = True
    _underscore_num = True
    _NS = Namespace("https://linked.data.gov.au/def/fl/")

    FeatureLabelComponent: URIRef
    FeatureLabelComponentType: URIRef
    FeatureLabelLifecycleStageType: URIRef
    LifecycleStage: URIRef

    isFeatureLabelFor: URIRef
    hasFeatureLabel: URIRef
    hasFeatureLabelComponent: URIRef
    hasComponentType: URIRef
    hasValue: URIRef
    hasValueText: URIRef


class RCT(DefinedNamespace):
    _fail = True
    _underscore_num = True
    _NS = Namespace("https://linked.data.gov.au/def/roads/ct/")

    RoadPrefix: URIRef
    RoadName: URIRef
    RoadType: URIRef
    RoadSuffix: URIRef


class LIFECYCLE(DefinedNamespace):
    _fail = True
    _underscore_num = True
    _NS = Namespace("https://linked.data.gov.au/def/lifecycle/")

    hasLifecycleStage: URIRef
    hasTime: URIRef


class LST(DefinedNamespace):
    _fail = True
    _underscore_num = True
    _NS = Namespace(
        "http://def.isotc211.org/iso19160/-1/2015/Address/code/AnzAddressLifecycleStageTypes/"
    )

    current: URIRef
    retired: URIRef


class CN(DefinedNamespace):
    _fail = True
    _underscore_num = True
    _NS = Namespace("https://linked.data.gov.au/def/cn/")

    CompoundName: URIRef
    isNameFor: URIRef


prefixes = {
    "addr": ADDR,
    "act": ADDRCMPType,
    "geo": GEO,
    "dcterms": DCTERMS,
    "skos": SKOS,
    "xsd": XSD,
    "astiso": ASTISO,
    "roads": ROADS,
    "fl": FL,
    "rct": RCT,
    "lifecycle": LIFECYCLE,
    "time": TIME,
    "lst": LST,
    "cn": CN,
    "sdo": SDO,
}


def create_graph(path: str):
    p = Path(path)
    if p.exists():
        shutil.rmtree(p)
    p.mkdir(parents=True)

    graph = Graph(store="Oxigraph")
    graph.open(path)

    for key, val in prefixes.items():
        graph.bind(key, val)

    return graph
