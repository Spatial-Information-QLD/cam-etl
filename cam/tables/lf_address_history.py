import itertools
from collections import defaultdict
from pathlib import Path

from rdflib import URIRef, Literal, BNode
from rdflib.namespace import RDFS, TIME, DCTERMS, PROV
from pyspark.sql import SparkSession
from jinja2 import Template

from cam.tables import Table
from cam.tables.lf_address import AddressTable
from cam.graph import LIFECYCLE, LST, create_graph


class AddressHistoryTable(Table):
    table = "lalfdb.lalfpdba_lf_address_history"

    def __init__(self, spark: SparkSession, site_ids: list[str] = None) -> None:
        super().__init__(spark)

        self.df = (
            spark.read.format("jdbc")
            .option(
                "url",
                "jdbc:postgresql://localhost:5432/address?user=postgres&password=postgres",
            )
            .option("driver", "org.postgresql.Driver")
            .option(
                "dbtable",
                Template(
                    """
                    (
                        select
                            a.site_id,
                            ah.addr_id,
                            ah.addr_history_id,
                            cast(ah.version_no as integer) as version,
                            to_timestamp(cast(cast(ah.addr_create_date as numeric) as text), 'YYYYMMDDHH24MISS') as created,
                            to_timestamp(cast(cast(audit.date_modified as numeric) as text), 'YYYYMMDDHH24MISS') as modified,
                            --to_timestamp(cast(cast(ah.addr_data_source_date as numeric) as text), 'YYYYMMDDHH24MISS') as source,
                            audit.audit_id,
                            audit.change_type as comment
                        from lalfdb.lalfpdba_lf_site s
                            join lalfdb.lalfpdba_lf_address a on a.site_id = s.site_id
                            join lalfdb.lalfpdba_lf_address_history ah on ah.addr_id = a.addr_id
                            join lalfdb."lalfpdba.lf_audit.csv" audit on audit.audit_id = ah.audit_id
                        where
                            a.addr_status_code != 'H'
                            {% if site_ids %}and a.site_id in {{ site_ids }}{% endif %}
                        order by version
                    ) AS address_history
                """,
                ).render(site_ids=site_ids),
            )
            .load()
        )

    @staticmethod
    def get_iri(addr_id: str, addr_history_id: str, version: str):
        return URIRef(
            f"https://linked.data.gov.au/dataset/qld-addr/lifecycle-{addr_id}-{version}-{addr_history_id}"
        )

    @staticmethod
    def transform(rows: itertools.chain, table_name: str):
        oxigraph_path = Path(f"oxigraph_data/{table_name}")
        graph = create_graph(str(oxigraph_path))

        ADDR_ID = "addr_id"
        ADDR_HISTORY_ID = "addr_history_id"
        VERSION = "version"
        COMMENT = "comment"
        CREATED = "created"
        MODIFIED = "modified"

        addresses = defaultdict(list)

        for row in rows:
            addresses[row[ADDR_ID]].append(row)

        for addr_id, address_histories in addresses.items():
            address_iri = AddressTable.get_iri(addr_id)

            for i, address_history in enumerate(address_histories):
                version = address_history[VERSION]
                is_current_lifecycle_stage = (len(address_histories) - 1) == i

                # lifecycle stage
                lifecycle_stage_iri = AddressHistoryTable.get_iri(
                    addr_id, address_history[ADDR_HISTORY_ID], version
                )
                prev_lifecycle_stage_iri = AddressHistoryTable.get_iri(
                    addr_id,
                    address_histories[i - 1][ADDR_HISTORY_ID],
                    address_histories[i - 1][VERSION],
                )
                graph.add(
                    (address_iri, LIFECYCLE.hasLifecycleStage, lifecycle_stage_iri)
                )

                # lifecycle stage label
                graph.add(
                    (
                        lifecycle_stage_iri,
                        RDFS.label,
                        Literal(
                            f"Lifecycle for addr {addr_id} stage {version} - {row[ADDR_HISTORY_ID]}"
                        ),
                    )
                )

                # lifecycle stage type
                if is_current_lifecycle_stage:
                    graph.add((lifecycle_stage_iri, DCTERMS.type, LST.current))
                else:
                    graph.add((lifecycle_stage_iri, DCTERMS.type, LST.retired))

                # lifecycle stage comment
                graph.add(
                    (
                        lifecycle_stage_iri,
                        RDFS.comment,
                        Literal(address_history[COMMENT]),
                    )
                )

                # lifecycle stage time
                time_object = BNode()
                time_beginning_object = BNode()
                graph.add((lifecycle_stage_iri, LIFECYCLE.hasTime, time_object))
                graph.add((time_object, TIME.hasBeginning, time_beginning_object))
                if version == 1:
                    graph.add(
                        (
                            time_beginning_object,
                            TIME.inXSDDate,
                            Literal(address_history[CREATED].date()),
                        )
                    )
                else:
                    graph.add(
                        (
                            time_beginning_object,
                            TIME.inXSDDate,
                            Literal(address_histories[i][MODIFIED].date()),
                        )
                    )

                if not is_current_lifecycle_stage:
                    time_end_object = BNode()
                    graph.add(
                        (
                            time_end_object,
                            TIME.inXSDDate,
                            Literal(address_history[MODIFIED]),
                        )
                    )
                    graph.add((time_object, TIME.hasEnd, time_end_object))

                # prov:wasInformedBy
                if version != 1:
                    graph.add(
                        (
                            lifecycle_stage_iri,
                            PROV.wasInformedBy,
                            prev_lifecycle_stage_iri,
                        )
                    )

        Table.to_file(table_name, graph)
        graph.close()
