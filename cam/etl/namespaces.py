from rdflib import URIRef
from rdflib.namespace import DefinedNamespace, Namespace


sir_id_datatype = URIRef("https://linked.data.gov.au/dataset/qld-addr/datatype/address-pid")
property_datatype = URIRef(
    "https://linked.data.gov.au/dataset/qld-addr/datatype/property"
)
lotplan_datatype = URIRef(
    "https://linked.data.gov.au/dataset/qld-addr/datatype/lotplan"
)
lot_datatype = URIRef("https://linked.data.gov.au/dataset/qld-addr/datatype/lot")
plan_datatype = URIRef("https://linked.data.gov.au/dataset/qld-addr/datatype/plan")
qld_state = URIRef("https://sws.geonames.org/2152274/")
aus_country = URIRef("https://sws.geonames.org/2077456/")
lifecycle_stage_current = URIRef(
    "https://linked.data.gov.au/def/lifecycle-stage-types/current"
)


class ADDR_PT(DefinedNamespace):
    _fail = True
    _underscore_num = True
    _NS = Namespace("https://linked.data.gov.au/def/addr-part-types/")

    addressNumberFirst: URIRef
    addressNumberFirstSuffix: URIRef
    addressNumberLast: URIRef
    addressNumberLastSuffix: URIRef
    buildingLevelNumber: URIRef
    buildingLevelNumberSuffix: URIRef
    buildingLevelType: URIRef
    countryName: URIRef
    geographicName: URIRef
    buildingName: URIRef
    indigenousCountryName: URIRef
    locality: URIRef
    postcode: URIRef
    propertyName: URIRef
    road: URIRef
    stateOrTerritory: URIRef
    subaddressNumber: URIRef
    subaddressNumberSuffix: URIRef
    subaddressType: URIRef
    waterFeature: URIRef
    thoroughfareName: URIRef


class ADDR(DefinedNamespace):
    _fail = True
    _underscore_num = True
    _NS = Namespace("https://linked.data.gov.au/def/addr/")

    Address: URIRef
    AddressableObject: URIRef

    hasGeocode: URIRef
    hasStatus: URIRef


class CN(DefinedNamespace):
    _fail = True
    _underscore_num = True
    _NS = Namespace("https://linked.data.gov.au/def/cn/")

    CompoundName: URIRef
    hasAuthority: URIRef
    hasName: URIRef
    isNameFor: URIRef
    nameTemplate: URIRef


class GN(DefinedNamespace):
    _fail = True
    _underscore_num = True
    _NS = Namespace("https://linked.data.gov.au/def/gn/")

    GeographicalName: URIRef
    GeographicalObject: URIRef


class GN_STATUS(DefinedNamespace):
    _fail = True
    _underscore_num = True
    _NS = Namespace("https://linked.data.gov.au/def/gn-statuses/")

    indigenous: URIRef
    informal: URIRef
    historical: URIRef
    official: URIRef
    gazetted: URIRef
    published: URIRef
    released: URIRef
    retired: URIRef


class GNPT(DefinedNamespace):
    _fail = True
    _underscore_num = True
    _NS = Namespace("https://linked.data.gov.au/def/gn-part-types/")

    geographicalGivenName: URIRef
    geographicalPrefix: URIRef
    geographicalSuffix: URIRef


class LC(DefinedNamespace):
    _fail = True
    _underscore_num = True
    _NS = Namespace("https://linked.data.gov.au/def/lifecycle/")

    hasLifecycleStage: URIRef


class REG(DefinedNamespace):
    _fail = True
    _underscore_num = True
    _NS = Namespace("https://linked.data.gov.au/def/reg-statuses/")

    accepted: URIRef
    notAccepted: URIRef
    retired: URIRef


class ROADS(DefinedNamespace):
    _fail = True
    _underscore_num = True
    _NS = Namespace("https://linked.data.gov.au/def/roads/")

    RoadName: URIRef
    RoadObject: URIRef
    RoadSegment: URIRef

    # TODO: remove this - it was only used for testing to check that the data was consistent.
    localityLeft: URIRef
    localityRight: URIRef
    lgaLeft: URIRef
    lgaRight: URIRef


class RNPT(DefinedNamespace):
    _fail = True
    _underscore_num = True
    _NS = Namespace("https://linked.data.gov.au/def/road-name-part-types/")

    RoadGivenName: URIRef
    RoadType: URIRef
    RoadSuffix: URIRef
