import time
from typing import Type

import rich
from pyspark.sql import SparkSession

from cam.config import Config
from cam.tables import (
    Table,
    lf_address,
    lf_geocode,
    lf_sp_survey_point,
    lf_status,
    locality,
    qrt,
    lf_address_history,
    placenm,
    lf_place_name,
    lf_parcel,
    lf_unit_type,
    lf_level_type,
)


table_module_mapping: dict[str, Type[Table]] = {
    "lalfdb.lalfpdba_lf_address": lf_address.AddressTable,
    "lalfdb.lalfpdba_lf_geocode": lf_geocode.GeocodeTable,
    "lalfdb.lalfpdba_sp_survey_point": lf_sp_survey_point.SPSurveyPointTable,
    "lalfdb.lalfpdba_lf_status": lf_status.StatusTable,
    "lalfdb.locality": locality.LocalityTable,
    "lalfdb.qrt": qrt.QRTRoadsTable,
    "lalfdb.lalfpdba_lf_address_history": lf_address_history.AddressHistoryTable,
    "lalfdb.lapnpdba_placenm": placenm.GazettedPlaceNmTable,
    "lalfdb.lalfpdba_lf_place_name": lf_place_name.PlacenameTable,
    "lalfdb.lalfpdba_lf_parcel": lf_parcel.ParcelTable,
    "lalfdb.lalfpdba_lf_unit_type": lf_unit_type.UnitTypeTable,
    "lalfdb.lalfpdba_lf_level_type": lf_level_type.LevelTypeTable,
}


def main():
    config = Config.parse_file("config.yml")
    rich.print(config)

    spark = (
        SparkSession.builder.config("spark.driver.memory", "8g")
        .config("spark.jars", "postgresql.jar")
        .getOrCreate()
    )

    for table in config.tables:
        database_table = table_module_mapping[table]
        database_table_instance = database_table(spark, config.site_ids)

        # database_table_instance.df = database_table_instance.df.repartition(12)
        database_table_instance.df.foreachPartition(
            lambda rows: database_table.transform(rows, table)
        )


if __name__ == "__main__":
    start_time = time.time()

    try:
        main()
    finally:
        processing_time = time.time() - start_time
        print(f"Completed in {processing_time:0.2f} seconds")
