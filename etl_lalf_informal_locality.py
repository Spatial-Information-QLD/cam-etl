from pathlib import Path

from rdflib import Dataset, URIRef

from cam.etl import serialize

dataset_name = "lalf_informal_locality"
output_dir_name = "lalf-rdf"
graph_name = URIRef("urn:qali:graph:geographical-names")

informal_localities_ttl = """
PREFIX cn: <https://linked.data.gov.au/def/cn/>
PREFIX gn: <https://linked.data.gov.au/def/gn/>
PREFIX gnpt: <https://linked.data.gov.au/def/gn-part-types/>
PREFIX gnst: <https://linked.data.gov.au/def/gn-statuses/>
PREFIX go-categories: <https://linked.data.gov.au/def/go-categories/>
PREFIX lm: <https://linked.data.gov.au/def/lifecycle/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX sdo: <https://schema.org/>
PREFIX time: <http://www.w3.org/2006/time#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

<https://linked.data.gov.au/dataset/qld-addr/geographic-name/13565322-ec74-594f-95cb-1eb6c34d3772>
    rdf:type cn:CompoundName, gn:GeographicalName ;
    cn:isNameFor <https://linked.data.gov.au/dataset/qld-addr/geographic-object/36352ab2-1536-5b53-8842-0e6d6791db28> ;
    lm:hasLifecycleStage [
        time:hasBeginning [
            time:inXSDDate "2003-10-25"^^xsd:date
        ] ;
        sdo:additionalType gnst:informal
    ] ;
    sdo:additionalProperty
        [ sdo:propertyID "pndb.currency" ; sdo:value "" ],
        [ sdo:propertyID "lalf.la_code" ; sdo:value "7340" ],
        [ sdo:propertyID "lalf.locality_code" ; sdo:value "LGA_3034" ],
        [ sdo:propertyID "lalf.state" ; sdo:value "QLD" ],
        [ sdo:propertyID "pndb.status" ; sdo:value "" ],
        [ sdo:propertyID "lalf.locality_type" ; sdo:value "DCDB" ],
        [ sdo:propertyID "postcode" ; sdo:value "" ],
        [ sdo:propertyID "lalf.locality_name" ; sdo:value "CORAL SEA" ],
        [ sdo:propertyID "pndb.plan_no" ; sdo:value "" ],
        [ sdo:propertyID "lalf.currency_status" ; sdo:value "C" ],
        [ sdo:propertyID "pndb.gazette_page" ; sdo:value "" ],
        [ sdo:propertyID "pndb.lga_name" ; sdo:value "" ],
        [ sdo:propertyID "pndb.origin" ; sdo:value "" ] ;
    sdo:hasPart [
        sdo:additionalType gnpt:geographicalGivenName ;
        sdo:value "CORAL SEA"@en
    ] ;
    sdo:name "CORAL SEA" .

<https://linked.data.gov.au/dataset/qld-addr/geographic-object/36352ab2-1536-5b53-8842-0e6d6791db28>
    rdf:type gn:GeographicalObject ;
    cn:hasName <https://linked.data.gov.au/dataset/qld-addr/geographic-name/13565322-ec74-594f-95cb-1eb6c34d3772> ;
    sdo:additionalType go-categories:locality ;
    sdo:identifier "LGA_3034"^^<https://linked.data.gov.au/dataset/qld-addr/datatype/cisp> .

<https://linked.data.gov.au/dataset/qld-addr/geographic-name/dc717f7a-73f8-5d41-8918-8627401ff10c>
    rdf:type cn:CompoundName, gn:GeographicalName ;
    cn:isNameFor <https://linked.data.gov.au/dataset/qld-addr/geographic-object/df6730de-ab55-5a50-b46f-18691a289e97> ;
    lm:hasLifecycleStage [
        time:hasBeginning [
            time:inXSDDate "2003-10-25"^^xsd:date
        ] ;
        sdo:additionalType gnst:informal
    ] ;
    sdo:additionalProperty
        [ sdo:propertyID "pndb.currency" ; sdo:value "" ],
        [ sdo:propertyID "lalf.la_code" ; sdo:value "4770" ],
        [ sdo:propertyID "lalf.locality_code" ; sdo:value "LGA_3128" ],
        [ sdo:propertyID "lalf.state" ; sdo:value "QLD" ],
        [ sdo:propertyID "pndb.status" ; sdo:value "" ],
        [ sdo:propertyID "lalf.locality_type" ; sdo:value "DCDB" ],
        [ sdo:propertyID "postcode" ; sdo:value "" ],
        [ sdo:propertyID "lalf.locality_name" ; sdo:value "CORAL SEA" ],
        [ sdo:propertyID "pndb.plan_no" ; sdo:value "" ],
        [ sdo:propertyID "lalf.currency_status" ; sdo:value "C" ],
        [ sdo:propertyID "pndb.gazette_page" ; sdo:value "" ],
        [ sdo:propertyID "pndb.lga_name" ; sdo:value "" ],
        [ sdo:propertyID "pndb.origin" ; sdo:value "" ] ;
    sdo:hasPart [
        sdo:additionalType gnpt:geographicalGivenName ;
        sdo:value "CORAL SEA"@en
    ] ;
    sdo:name "CORAL SEA" .

<https://linked.data.gov.au/dataset/qld-addr/geographic-object/df6730de-ab55-5a50-b46f-18691a289e97>
    rdf:type gn:GeographicalObject ;
    cn:hasName <https://linked.data.gov.au/dataset/qld-addr/geographic-name/dc717f7a-73f8-5d41-8918-8627401ff10c> ;
    sdo:additionalType go-categories:locality ;
    sdo:identifier "LGA_3128"^^<https://linked.data.gov.au/dataset/qld-addr/datatype/cisp> .

<https://linked.data.gov.au/dataset/qld-addr/geographic-name/125f4533-46d1-5ebc-9603-7e027b6ab538>
    rdf:type cn:CompoundName, gn:GeographicalName ;
    cn:isNameFor <https://linked.data.gov.au/dataset/qld-addr/geographic-object/054463f2-d854-576f-a358-ef056fbcbacc> ;
    lm:hasLifecycleStage [
        time:hasBeginning [
            time:inXSDDate "2014-01-01"^^xsd:date
        ] ;
        sdo:additionalType gnst:informal
    ] ;
    sdo:additionalProperty
        [ sdo:propertyID "pndb.currency" ; sdo:value "" ],
        [ sdo:propertyID "lalf.la_code" ; sdo:value "2810" ],
        [ sdo:propertyID "lalf.locality_code" ; sdo:value "LGA_4008" ],
        [ sdo:propertyID "lalf.state" ; sdo:value "QLD" ],
        [ sdo:propertyID "pndb.status" ; sdo:value "" ],
        [ sdo:propertyID "lalf.locality_type" ; sdo:value "LOCB" ],
        [ sdo:propertyID "postcode" ; sdo:value "" ],
        [ sdo:propertyID "lalf.locality_name" ; sdo:value "CORAL SEA" ],
        [ sdo:propertyID "pndb.plan_no" ; sdo:value "" ],
        [ sdo:propertyID "lalf.currency_status" ; sdo:value "C" ],
        [ sdo:propertyID "pndb.gazette_page" ; sdo:value "" ],
        [ sdo:propertyID "pndb.lga_name" ; sdo:value "" ],
        [ sdo:propertyID "pndb.origin" ; sdo:value "" ] ;
    sdo:hasPart [
        sdo:additionalType gnpt:geographicalGivenName ;
        sdo:value "CORAL SEA"@en
    ] ;
    sdo:name "CORAL SEA" .

<https://linked.data.gov.au/dataset/qld-addr/geographic-object/054463f2-d854-576f-a358-ef056fbcbacc>
    rdf:type gn:GeographicalObject ;
    cn:hasName <https://linked.data.gov.au/dataset/qld-addr/geographic-name/125f4533-46d1-5ebc-9603-7e027b6ab538> ;
    sdo:additionalType go-categories:locality ;
    sdo:identifier "LGA_4008"^^<https://linked.data.gov.au/dataset/qld-addr/datatype/cisp> .
"""


def main():
    ds = Dataset(store="Oxigraph")
    ds.graph(graph_name).parse(data=informal_localities_ttl, format="turtle")

    output_dir = Path(output_dir_name)
    filename = Path(f"{dataset_name}-1.nq")
    serialize(output_dir, str(filename), ds)


if __name__ == "__main__":
    main()
