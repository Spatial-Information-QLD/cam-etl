import itertools
from datetime import datetime

from rdflib import Graph, URIRef, Literal
from rdflib.namespace import RDF, SKOS, DCTERMS
from pyspark.sql import SparkSession

from etl_lib.tables import Table


class StatusTable(Table):
    table = "lalfdb.lalfpdba_lf_status"

    STATUS_CODE = "status_code"
    STATUS = "status"

    def __init__(self, spark: SparkSession, limit: int = None) -> None:
        super().__init__(spark, limit)

    @staticmethod
    def get_iri(status_code: str):
        return URIRef(f"https://linked.data.gov.au/def/qld-addr-status/{status_code}")

    @staticmethod
    def transform(rows: itertools.chain, graph: Graph, table_name: str):
        concept_scheme = URIRef("https://linked.data.gov.au/def/qld-addr-status")
        graph.add((concept_scheme, RDF.type, SKOS.ConceptScheme))
        graph.add((concept_scheme, SKOS.prefLabel, Literal("Addressing Status Types")))
        current_time = datetime.now()
        graph.add((concept_scheme, DCTERMS.created, Literal(current_time)))
        graph.add((concept_scheme, DCTERMS.modified, Literal(current_time)))
        provenance_text = (
            "Generated by the Cadastre and Addressing Modernisation project."
        )
        graph.add((concept_scheme, DCTERMS.provenance, Literal(provenance_text)))
        graph.add(
            (
                concept_scheme,
                SKOS.definition,
                Literal("Status types for Queensland addresses."),
            )
        )

        for row in rows:
            concept = URIRef(StatusTable.get_iri(row[StatusTable.STATUS_CODE]))
            graph.add((concept_scheme, SKOS.hasTopConcept, concept))

            graph.add((concept, RDF.type, SKOS.Concept))
            graph.add((concept, DCTERMS.provenance, Literal(provenance_text)))
            graph.add((concept, SKOS.prefLabel, Literal(row[StatusTable.STATUS])))
            graph.add((concept, SKOS.definition, Literal(row[StatusTable.STATUS])))
            graph.add((concept, SKOS.inScheme, concept_scheme))

        Table.to_file(table_name, graph)
