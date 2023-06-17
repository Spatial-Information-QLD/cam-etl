from textwrap import dedent

import requests
from jinja2 import Template


def get_compound_name_object(iri: str, sparql_endpoint: str) -> dict:
    headers = {
        "accept": "application/sparql-results+json",
        "content-type": "application/sparql-query",
    }

    query = Template(
        """
        PREFIX func: <https://linked.data.gov.au/def/cn/func/>
        SELECT *
        WHERE {
            BIND(<{{ iri }}> AS ?compoundNameObject)

            ?compoundNameObject func:getLiteralComponents (?componentType ?componentValue) .
        }
    """
    ).render(iri=iri)

    response = requests.post(sparql_endpoint, data=query, headers=headers, timeout=60)

    status_code = response.status_code
    if status_code != 200:
        raise Exception(
            f"Received response code {status_code} with message '{response.text}'"
        )

    return response.json()


def template_address(sparql_result: dict) -> str:
    components = {}

    for row in sparql_result["results"]["bindings"]:
        components[row["componentType"]["value"]] = row["componentValue"]["value"]

    lines = (
        Template(
            dedent(
                """
                {% if 'https://linked.data.gov.au/def/gn-part-types/GeographicalGivenName' in components %}{{ components['https://linked.data.gov.au/def/gn-part-types/GeographicalGivenName'] }}{% endif %}
                {% if 'https://w3id.org/profile/anz-address/AnzAddressComponentTypes/flatTypeCode' in components %}{{ components['https://w3id.org/profile/anz-address/AnzAddressComponentTypes/flatTypeCode'] }}{% endif %} {% if 'https://w3id.org/profile/anz-address/AnzAddressComponentTypes/flatNumber' in components %}{{ components['https://w3id.org/profile/anz-address/AnzAddressComponentTypes/flatNumber'] }}{% endif %}{% if 'https://w3id.org/profile/anz-address/AnzAddressComponentTypes/flatNumberSuffix' in components %}{{ components['https://w3id.org/profile/anz-address/AnzAddressComponentTypes/flatNumberSuffix'] }}{% endif %}
                {% if 'https://w3id.org/profile/anz-address/AnzAddressComponentTypes/levelTypeCode' in components %}{{ components['https://w3id.org/profile/anz-address/AnzAddressComponentTypes/levelTypeCode'] }}{% endif %} {% if 'https://w3id.org/profile/anz-address/AnzAddressComponentTypes/levelNumber' in components %}{{ components['https://w3id.org/profile/anz-address/AnzAddressComponentTypes/levelNumber'] }}{% endif %}
                {% if 'https://w3id.org/profile/anz-address/AnzAddressComponentTypes/numberFirst' in components %}{{ components['https://w3id.org/profile/anz-address/AnzAddressComponentTypes/numberFirst'] }}{% endif %}{% if 'https://w3id.org/profile/anz-address/AnzAddressComponentTypes/numberFirstSuffix' in components %}{{ components['https://w3id.org/profile/anz-address/AnzAddressComponentTypes/numberFirstSuffix'] }}{% endif %}{% if 'https://w3id.org/profile/anz-address/AnzAddressComponentTypes/numberLast' in components %}-{{ components['https://w3id.org/profile/anz-address/AnzAddressComponentTypes/numberLast'] }}{% endif %}{% if 'https://w3id.org/profile/anz-address/AnzAddressComponentTypes/numberLastSuffix' in components %}{{ components['https://w3id.org/profile/anz-address/AnzAddressComponentTypes/numberLastSuffix'] }}{% endif %} {% if 'https://linked.data.gov.au/def/roads/ct/RoadPrefix in components %}{{ components['https://linked.data.gov.au/def/roads/ct/RoadPrefix'] }}{% endif %} {% if 'https://linked.data.gov.au/def/roads/ct/RoadName' in components %}{{ components['https://linked.data.gov.au/def/roads/ct/RoadName'] }}{% endif %} {% if 'https://linked.data.gov.au/def/roads/ct/RoadType' in components %}{{ components['https://linked.data.gov.au/def/roads/ct/RoadType'] }}{% endif %} {% if 'https://linked.data.gov.au/def/roads/ct/RoadSuffix' in components %}{{ components['https://linked.data.gov.au/def/roads/ct/RoadSuffix'] }}{% endif %}
                {% if 'https://w3id.org/profile/anz-address/AnzAddressComponentTypes/locality' in components %}{{ components['https://w3id.org/profile/anz-address/AnzAddressComponentTypes/locality'] }}{% endif %}
                {% if 'https://w3id.org/profile/anz-address/AnzAddressComponentTypes/stateOrTerritory' in components %}{{ components['https://w3id.org/profile/anz-address/AnzAddressComponentTypes/stateOrTerritory'] }}{% endif %} {% if 'http://def.isotc211.org/iso19160/-1/2015/Address/code/AnzAddressComponentTypes/postcode' in components %}{{ components['http://def.isotc211.org/iso19160/-1/2015/Address/code/AnzAddressComponentTypes/postcode'] }}{% endif %}
                {% if 'http://def.isotc211.org/iso19160/-1/2015/Address/code/AnzAddressComponentTypes/countryName' in components %}{{ components['http://def.isotc211.org/iso19160/-1/2015/Address/code/AnzAddressComponentTypes/countryName'] }}{% endif %}
                """
            )
        )
        .render(components=components)
        .strip()
    )

    # Remove empty blank lines before returning the value.
    lines_list = [line for line in lines.split("\n") if line.strip()]
    return "\n".join(lines_list)
