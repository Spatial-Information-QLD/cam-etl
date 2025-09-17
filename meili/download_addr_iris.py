import httpx
from rdflib import Namespace

SPARQL_ENDPOINT = "http://localhost:3030/qali/query"
QUERY = """
PREFIX 	cn: <https://linked.data.gov.au/def/cn/>
PREFIX  rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX  text: <http://jena.apache.org/text#>
PREFIX  addr: <https://linked.data.gov.au/def/addr/>
PREFIX  prov: <http://www.w3.org/ns/prov#>
PREFIX  sdo:  <https://schema.org/>

CONSTRUCT {
    ?iri a addr:Address .
}
FROM <urn:ladb:graph:addresses>
where {  
  ?iri a addr:Address .
}
"""

CN = Namespace("https://linked.data.gov.au/def/cn/")
GN = Namespace("https://linked.data.gov.au/def/gn/")
LC = Namespace("https://linked.data.gov.au/def/lifecycle/")


def main():
    with open("data_address_iris.nt", "w", encoding="utf-8") as file:
        with httpx.stream(
            "POST",
            SPARQL_ENDPOINT,
            data=QUERY,
            headers={
                "Content-Type": "application/sparql-query",
                "Accept": "application/n-triples",
            },
            timeout=None,
        ) as response:
            for chunk in response.iter_text():
                file.write(chunk)


if __name__ == "__main__":
    main()
