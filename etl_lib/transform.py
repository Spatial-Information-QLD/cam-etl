import os
import itertools
from pathlib import Path

from rdflib import Graph, URIRef, Literal, Namespace, BNode
from rdflib.namespace import RDF

from etl_lib.config import Table, IRI, LITERAL


def expand_curie(curie: str, prefixes: dict):
    local_name = curie.split(":")[-1]
    prefix = curie.replace(f":{local_name}", "")
    return prefixes[prefix] + local_name


def interpolate_iri_template(iri_template: str, field_mapping: dict | list, row):
    iri = iri_template

    if isinstance(field_mapping, list):
        for column in field_mapping:
            template_val = f"{{{column}}}"
            if template_val in iri:
                try:
                    value = row[column]
                except ValueError:
                    raise ValueError(
                        f"Failed to get value from column {column} from row {row}"
                    )
                iri = iri.replace(template_val, str(value))
    elif isinstance(field_mapping, dict):
        for column, val in field_mapping.items():
            template_val = f"{{{column}}}"
            if template_val in iri:
                iri = iri.replace(template_val, str(val))
    else:
        raise TypeError(
            f"Expected field_mapping to be a dict or list, but got {type(field_mapping)} instead."
        )

    if "{" in iri or "}" in iri:
        raise RuntimeError(
            f"Field mapping missing value for iri_template {iri_template}. {field_mapping}"
        )

    return iri


def get_new_graph(prefixes: dict):
    graph = Graph()

    for key, val in prefixes.items():
        graph.bind(key, Namespace(val))

    return graph


def transform(
    rows: itertools.chain,
    table_name: str,
    table: Table,
    prefixes: dict[str, str],
    tables: dict[str, Table],
):
    types = [expand_curie(t, prefixes) for t in table.instance.types]
    graph = get_new_graph(prefixes)

    for row in rows:
        iri_result = interpolate_iri_template(
            table.instance.iri_template, table.instance.field_mapping, row
        )
        if table.instance.blank_node:
            iri = BNode(iri_result)
        else:
            iri = URIRef(iri_result)
        for t in types:
            graph.add((iri, RDF.type, URIRef(t)))

        if table.columns:
            for column_name, column in table.columns.items():
                predicate = URIRef(expand_curie(column.predicate, prefixes))
                if isinstance(column.object, IRI):
                    mapping = {}
                    for key, val in column.object.field_mapping.items():
                        mapping.update({key: row[val]})
                    obj_iri = interpolate_iri_template(
                        tables[column.object.table].instance.iri_template,
                        mapping,
                        row,
                    )

                    if tables[column.object.table].instance.blank_node:
                        obj_iri = BNode(obj_iri)
                    else:
                        obj_iri = URIRef(obj_iri)

                    graph.add((iri, predicate, obj_iri))

                elif isinstance(column.object, LITERAL):
                    datatype = expand_curie(column.object.datatype, prefixes)

                    graph.add(
                        (
                            iri,
                            predicate,
                            Literal(
                                row[column_name],
                                datatype=datatype,
                                lang=column.object.lang,
                            ),
                        )
                    )
                else:
                    raise ValueError(f"Invalid type received. {type(column.object)}")

    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)
    filename = Path(table_name + str(os.getpid()) + ".ttl")
    graph.serialize(output_dir / filename, format="turtle")
