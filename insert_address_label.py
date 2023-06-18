import time
import logging

from jinja2 import Template

from cam.graphdb import sparql, sparql_update
from cam.compound_naming import get_compound_name_object, template_address_one_liner


def main() -> None:
    graphdb_url = "http://localhost:7200"
    repository_id = "addressing"

    query = """
        SELECT ?iri
        WHERE {
            VALUES ?iri {
                <https://linked.data.gov.au/dataset/qld-addr/addr-1066374>
                <https://linked.data.gov.au/dataset/qld-addr/addr-2285769>
                <https://linked.data.gov.au/dataset/qld-addr/addr-1075435>
                <https://linked.data.gov.au/dataset/qld-addr/addr-2522774>
                <https://linked.data.gov.au/dataset/qld-addr/addr-1724075>
                <https://linked.data.gov.au/dataset/qld-addr/addr-2819296>
                <https://linked.data.gov.au/dataset/qld-addr/addr-82599>
                <https://linked.data.gov.au/dataset/qld-addr/addr-2367581>
                <https://linked.data.gov.au/dataset/qld-addr/addr-347127>
            }
        }
    """

    results = sparql(graphdb_url, repository_id, query)
    iris = [row["iri"]["value"] for row in results["results"]["bindings"]]

    for iri in iris:
        compound_name_object = get_compound_name_object(
            iri, f"{graphdb_url}/repositories/{repository_id}"
        )
        address = template_address_one_liner(compound_name_object)

        if address != "":
            update_query = Template(
                """
                PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                INSERT DATA {
                    GRAPH <urn:address:labels> {
                        <{{ iri }}> rdfs:label "{{ address }}" .
                    }
                }
            """
            ).render(iri=iri, address=address)
            sparql_update(graphdb_url, repository_id, update_query)
        else:
            logging.warning(f"Could not template address for {iri}.")


if __name__ == "__main__":
    starttime = time.time()

    try:
        main()
    finally:
        endtime = time.time() - starttime
        print(f"Completed in {endtime:0.2f} seconds")
