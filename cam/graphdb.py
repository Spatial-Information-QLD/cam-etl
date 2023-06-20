import requests
from jinja2 import Template


def autocomplete(graphdb_url: str, repository_id: str, enabled: bool) -> None:
    """Enable or disable autocomplete on repository.

    Always disables IRI indexing.

    If enabling autocomplete, an index action will be triggered automatically.
    """
    headers = {"X-GraphDB-Repository": repository_id}

    # Disable IRI indexing
    response = requests.post(
        f"{graphdb_url}/rest/autocomplete/iris",
        params={"enabled": "false"},
        headers=headers,
    )
    response.raise_for_status()

    # Enable or disable autocomplete
    params = {"enabled": enabled}
    response = requests.post(
        f"{graphdb_url}/rest/autocomplete/enabled", params=params, headers=headers
    )
    response.raise_for_status()


def reindex(graphdb_url: str, repository_id: str) -> None:
    headers = {"X-GraphDB-Repository": repository_id}
    response = requests.post(
        f"{graphdb_url}/rest/autocomplete/reIndex", headers=headers
    )
    response.raise_for_status()


def sparql(graphdb_url: str, repository_id: str, query: str) -> dict:
    headers = {
        "accept": "application/sparql-results+json",
        "content-type": "application/sparql-query",
    }
    response = requests.post(
        f"{graphdb_url}/repositories/{repository_id}",
        data=query,
        headers=headers,
    )
    response.raise_for_status()
    return response.json()


def sparql_update(graphdb_url: str, repository_id: str, query: str) -> None:
    response = requests.post(
        f"{graphdb_url}/repositories/{repository_id}/statements",
        data={"update": query},
    )
    response.raise_for_status()


def sparql_describe(iri: str, graphdb_url: str, repository_id: str) -> str:
    headers = {"accept": "text/turtle", "content-type": "application/sparql-query"}
    query = Template(
        """
        describe <{{ iri }}>
    """
    ).render(iri=iri)
    response = requests.post(
        f"{graphdb_url}/repositories/{repository_id}", data=query, headers=headers
    )
    response.raise_for_status()
    return response.text
