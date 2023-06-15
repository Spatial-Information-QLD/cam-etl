import itertools
from datetime import datetime
from pathlib import Path

from rdflib import URIRef, Literal
from rdflib.namespace import RDF, SKOS, DCTERMS, XSD
from pyspark.sql import SparkSession

from cam.tables import Table
from cam.graph import ASTISO, create_graph

anz_address_types = """
PREFIX astiso: <http://def.isotc211.org/iso19160/-1/2015/Address/code/AnzAddressStatusTypes/>
PREFIX astisocs: <http://def.isotc211.org/iso19160/-1/2015/Address/code/AnzAddressStatusTypes>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

astiso:official
    a skos:Concept ;
    dcterms:identifier "official"^^xsd:token ;
    rdfs:isDefinedBy astisocs: ;
    skos:definition "An official addressing authority assigned the address" ;
    skos:inScheme <https://linked.data.gov.au/def/qld-addr-status> ;
    skos:prefLabel "official"@en ;
    skos:topConceptOf <https://linked.data.gov.au/def/qld-addr-status> ;
.

astiso:unknown
    a skos:Concept ;
    dcterms:identifier "unknown"^^xsd:token ;
    rdfs:isDefinedBy astisocs: ;
    skos:definition "The status of the address is unknown" ;
    skos:inScheme <https://linked.data.gov.au/def/qld-addr-status> ;
    skos:prefLabel "unknown"@en ;
    skos:topConceptOf <https://linked.data.gov.au/def/qld-addr-status> ;
.

astiso:unofficial
    a skos:Concept ;
    dcterms:identifier "unofficial"^^xsd:token ;
    rdfs:isDefinedBy astisocs: ;
    skos:definition "The address was not assigned by an official addressing authority" ;
    skos:inScheme <https://linked.data.gov.au/def/qld-addr-status> ;
    skos:prefLabel "unofficial"@en ;
    skos:topConceptOf <https://linked.data.gov.au/def/qld-addr-status> ;
.
"""


class StatusTable(Table):
    table = "lalfdb.lalfpdba_lf_status"

    STATUS_CODE = "status_code"
    STATUS = "status"

    def __init__(self, spark: SparkSession, site_ids: str = None) -> None:
        super().__init__(spark)

    @staticmethod
    def get_iri(status_code: str):
        return URIRef(f"https://linked.data.gov.au/def/qld-addr-status/{status_code}")

    @staticmethod
    def transform(rows: itertools.chain, table_name: str):
        oxigraph_path = Path(f"oxigraph_data/{table_name}")
        graph = create_graph(str(oxigraph_path))

        concept_scheme = URIRef("https://linked.data.gov.au/def/qld-addr-status")
        graph.add((concept_scheme, RDF.type, SKOS.ConceptScheme))
        graph.add(
            (
                concept_scheme,
                SKOS.prefLabel,
                Literal("Addressing Status Types", lang="en"),
            )
        )
        current_time = datetime.now()
        graph.add((concept_scheme, DCTERMS.created, Literal(current_time)))
        graph.add((concept_scheme, DCTERMS.modified, Literal(current_time)))
        provenance_text = (
            "Generated by the Cadastre and Addressing Modernisation project."
        )
        graph.add(
            (concept_scheme, DCTERMS.provenance, Literal(provenance_text, lang="en"))
        )
        graph.add(
            (
                concept_scheme,
                SKOS.definition,
                Literal("Status types for Queensland addresses.", lang="en"),
            )
        )

        for row in rows:
            concept = URIRef(StatusTable.get_iri(row[StatusTable.STATUS_CODE]))

            graph.add((concept, RDF.type, SKOS.Concept))
            graph.add(
                (
                    concept,
                    DCTERMS.identifier,
                    Literal(row[StatusTable.STATUS_CODE], datatype=XSD.token),
                )
            )
            graph.add(
                (concept, DCTERMS.provenance, Literal(provenance_text, lang="en"))
            )
            graph.add(
                (concept, SKOS.prefLabel, Literal(row[StatusTable.STATUS], lang="en"))
            )
            graph.add(
                (concept, SKOS.definition, Literal(row[StatusTable.STATUS], lang="en"))
            )
            graph.add((concept, SKOS.inScheme, concept_scheme))

        graph.parse(data=anz_address_types)

        # astiso:official
        graph.add((ASTISO.official, SKOS.narrower, StatusTable.get_iri("P")))
        graph.add((concept_scheme, SKOS.hasTopConcept, ASTISO.official))

        # astiso:unknown
        graph.add((ASTISO.unknown, SKOS.narrower, StatusTable.get_iri("U")))
        graph.add((concept_scheme, SKOS.hasTopConcept, ASTISO.unknown))

        # astiso:unofficial
        graph.add((ASTISO.unofficial, SKOS.narrower, StatusTable.get_iri("A")))
        graph.add((ASTISO.unofficial, SKOS.narrower, StatusTable.get_iri("H")))
        graph.add((ASTISO.unofficial, SKOS.narrower, StatusTable.get_iri("U")))
        graph.add((concept_scheme, SKOS.hasTopConcept, ASTISO.unofficial))

        Table.to_file(table_name, graph)
        graph.close()
