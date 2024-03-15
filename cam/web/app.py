from os import environ

import requests
from flask import Flask, render_template, request
from flask_htmx import HTMX
from pydantic import BaseModel
from jinja2 import Template
from shapely.wkt import loads
from rdflib import Graph

from cam.compound_naming import (
    get_compound_name_object,
    template_address,
    template_address_one_liner,
)
from cam.graphdb import sparql, sparql_describe
from cam.graph import prefixes

app = Flask(__name__)
htmx = HTMX(app)

GRAPHDB_URL = environ.get("GRAPHDB_URL", "http://localhost:7200")
GRAPHDB_REPO = environ.get("GRAPHDB_REPO", "addressing")


class ResultItem(BaseModel):
    type: str
    value: str
    description: str


class Point(BaseModel):
    latitude: str
    longitude: str


def search(query: str) -> list[ResultItem]:
    params = {"q": query}
    headers = {"Accept": "application/json", "X-Graphdb-Repository": GRAPHDB_REPO}
    response = requests.get(
        f"{GRAPHDB_URL}/rest/autocomplete/query", params=params, headers=headers
    )
    response.raise_for_status()
    return [ResultItem(**item) for item in response.json()["suggestions"]]


def get_wkt_point(iri: str) -> Point:
    query = Template(
        """
        PREFIX func: <https://linked.data.gov.au/def/cn/func/>
        PREFIX addr: <https://w3id.org/profile/anz-address/>
        SELECT *
        WHERE {
            BIND(<{{ iri }}> AS ?iri)
            
            ?iri addr:hasGeocode ?geocode .
            ?geocode <http://www.opengis.net/ont/geosparql#hasGeometry> ?geo .
            ?geo <http://www.opengis.net/ont/geosparql#asWKT> ?wkt .
        }
    """
    ).render(iri=iri)

    results = sparql(GRAPHDB_URL, GRAPHDB_REPO, query)
    for row in results["results"]["bindings"]:
        wkt = row["wkt"]["value"]
        point = loads(wkt)
        return Point(longitude=point.x, latitude=point.y)


def long_turtle(value: str) -> str:
    graph = Graph()
    graph.parse(data=value)
    for key, val in prefixes.items():
        graph.bind(key, val)
    return graph.serialize(format="longturtle")


@app.get("/")
def home_route():
    return render_template("index.html")


@app.post("/search")
def search_route():
    query = request.form.get("search")
    results = search(query)
    size = len(results)
    if htmx:
        return render_template("partials/search.html", results=results, size=size)

    return home_route()


@app.get("/address")
def address_route():
    iri = request.args.get("iri")
    compound_name_object = get_compound_name_object(
        iri, f"{GRAPHDB_URL}/repositories/{GRAPHDB_REPO}"
    )
    address_one_liner = template_address_one_liner(compound_name_object)
    address_multi_liner = template_address(compound_name_object)
    point = get_wkt_point(iri)
    turtle_representation = long_turtle(sparql_describe(iri, GRAPHDB_URL, GRAPHDB_REPO))

    return render_template(
        "address.html",
        iri=iri,
        address_one_liner=address_one_liner,
        address_multi_liner=address_multi_liner,
        point=point,
        turtle_representation=turtle_representation,
        graphdb_url=GRAPHDB_URL,
    )
