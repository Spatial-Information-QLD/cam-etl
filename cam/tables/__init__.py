import os
import itertools
from abc import ABC, abstractmethod
from pathlib import Path

from rdflib import Graph
from pyoxigraph import Store, serialize
from pyspark.sql import SparkSession, DataFrame


class Table(ABC):
    table: str
    df: DataFrame

    def __init__(self, spark: SparkSession) -> None:
        self.df = (
            spark.read.format("jdbc")
            .option(
                "url",
                "jdbc:postgresql://localhost:5432/address?user=postgres&password=postgres",
            )
            .option("driver", "org.postgresql.Driver")
            .option("dbtable", self.table)
            .load()
        )

    @staticmethod
    @abstractmethod
    def transform(rows: itertools.chain, graph: Graph, table_name: str):
        ...

    @staticmethod
    def to_file(table_name: str, graph: Graph):
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        filename = Path(table_name + "-" + str(os.getpid()) + ".ttl")

        store: Store = graph.store._inner
        quads = store.quads_for_pattern(None, None, None)
        serialize(quads, str(output_dir / filename), "application/n-quads")
