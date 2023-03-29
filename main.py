import time

import rich
from pyspark.sql import SparkSession

from etl_lib.config import Config
from etl_lib.tables import (
    Table,
    lf_site,
    lf_address,
    lf_geocode,
    lf_sp_survey_point,
    lf_status,
)
from etl_lib.graph import get_new_graph


table_module_mapping = {
    "lalfdb.lalfpdba_lf_site": lf_site.SiteTable,
    "lalfdb.lalfpdba_lf_address": lf_address.AddressTable,
    "lalfdb.lalfpdba_lf_geocode": lf_geocode.GeocodeTable,
    "lalfdb.lalfpdba_sp_survey_point": lf_sp_survey_point.SPSurveyPointTable,
    "lalfdb.lalfpdba_lf_status": lf_status.StatusTable,
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
        database_table_instance: Table = database_table(spark, limit=config.limit)

        graph = get_new_graph()
        database_table_instance.df.foreachPartition(
            lambda rows: database_table.transform(rows, graph, table)
        )


if __name__ == "__main__":
    start_time = time.time()
    main()
    processing_time = time.time() - start_time
    print(f"Completed in {processing_time:0.2f} seconds")
