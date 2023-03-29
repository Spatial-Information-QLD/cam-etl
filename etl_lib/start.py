import time

import rich
from pyspark.sql import SparkSession

from etl_lib import ETLConfig, transform


def main():
    etl_config = ETLConfig.parse_file("etl.yml")

    rich.print(etl_config)

    spark = SparkSession.builder.config("spark.jars", "postgresql.jar").getOrCreate()

    tables = etl_config.etl.tables

    for table_name, table in tables.items():
        df = (
            spark.read.format("jdbc")
            .option(
                "url",
                "jdbc:postgresql://localhost:5432/address?user=postgres&password=postgres",
            )
            .option("driver", "org.postgresql.Driver")
            .option("dbtable", table_name)
            .load()
        )

        df.printSchema()

        filtered_df = df
        for key, val in table.instance.filters.items():
            filtered_df = df.filter(df[key] == val)

        filtered_df.foreachPartition(
            lambda rows: transform(rows, table_name, table, etl_config.prefixes, tables)
        )


if __name__ == "__main__":
    start_time = time.time()
    main()
    processing_time = time.time() - start_time
    print(f"Completed in {processing_time:0.2f} seconds")
