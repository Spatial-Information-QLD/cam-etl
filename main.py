import time
from typing import Type

import rich
from pyspark.sql import SparkSession

from cam.config import Config
from cam.tables import (
    Table,
    lf_site,
    lf_address,
    lf_geocode,
    lf_sp_survey_point,
    lf_status,
    locality,
    qrt,
    lf_address_history,
)
from cam.graph import create_graph


table_module_mapping: dict[str, Type[Table]] = {
    "lalfdb.lalfpdba_lf_site": lf_site.SiteTable,
    "lalfdb.lalfpdba_lf_address": lf_address.AddressTable,
    "lalfdb.lalfpdba_lf_geocode": lf_geocode.GeocodeTable,
    "lalfdb.lalfpdba_sp_survey_point": lf_sp_survey_point.SPSurveyPointTable,
    "lalfdb.lalfpdba_lf_status": lf_status.StatusTable,
    "lalfdb.locality": locality.LocalityTable,
    "lalfdb.qrt": qrt.QRTRoadsTable,
    "lalfdb.lalfpdba_lf_address_history": lf_address_history.AddressHistoryTable,
}


def main():
    config = Config.parse_file("config.yml")
    rich.print(config)

    spark = (
        SparkSession.builder.config("spark.driver.memory", "4g")
        .config("spark.jars", "postgresql.jar")
        .getOrCreate()
    )

    for table in config.tables:
        database_table = table_module_mapping[table]
        database_table_instance = database_table(
            spark, "(1066374, 1075435, 2578313, 1724075, 33254, 1837741)"
        )

        graph = create_graph()
        database_table_instance.df.foreachPartition(
            lambda rows: database_table.transform(rows, graph, table)
        )


if __name__ == "__main__":
    start_time = time.time()
    main()
    processing_time = time.time() - start_time
    print(f"Completed in {processing_time:0.2f} seconds")
