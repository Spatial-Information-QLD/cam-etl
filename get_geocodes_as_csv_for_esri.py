"""
Get geocodes as CSV for ESRI.
"""

import csv
import time
import logging
from textwrap import dedent

from psycopg.rows import Row

from cam.etl import get_db_connection
from cam.etl.settings import settings


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def process_rows(rows: list[Row], writer: csv.writer):
    for row in rows:
        writer.writerow(
            [
                row["geocode_type"],
                row["address_pid"],
                row["property_name"],
                row["building_name"],
                row["comments"],
                row["assoc_lotplans"],
                row["geocode_source"],
                row["address"],
                row["address_status"],
                row["longitude"],
                row["latitude"],
            ]
        )


def main():
    with get_db_connection(
        host=settings.etl.db.host,
        port=settings.etl.db.port,
        dbname=settings.etl.db.name,
        user=settings.etl.db.user,
        password=settings.etl.db.password,
    ) as connection:

        with connection.cursor(name="main", scrollable=False) as cursor:

            cursor.itersize = settings.etl.batch_size
            cursor.execute(
                dedent(
                    """\
                    SELECT
                        g.geocode_type_code AS geocode_type,
                        a.addr_id AS address_pid,
                        NULL AS property_name,
                        NULL AS building_name,
                        'Loaded from LALF' AS comments,
                        concat(p.lot_no, p.plan_no) AS assoc_lotplans,
                        'LALF' AS geocode_source,
                        NULL AS address,
                        a.addr_status_code AS address_status,
                        sp.centroid_lon AS longitude,
                        sp.centroid_lat AS latitude
                    FROM "lalfpdba.lf_geocode" g
                    JOIN "lalfpdba.lf_address" a ON g.site_id = a.site_id
                    JOIN "lalfpdba.sp_survey_point" sp ON g.spdb_pid = sp.pid
                    JOIN "lalfpdba.lf_site" s ON g.site_id = s.site_id
                    JOIN "lalfpdba.lf_parcel" p ON s.parcel_id = p.parcel_id
                    WHERE
                        g.geocode_status_code != 'H'
                        AND a.addr_status_code != 'H'
                """
                ),
            )

            with open("geocodes_for_esri.csv", "w") as f:
                writer = csv.writer(f)
                writer.writerow(
                    [
                        "geocode_type",
                        "address_pid",
                        "property_name",
                        "building_name",
                        "comments",
                        "assoc_lotplans",
                        "geocode_source",
                        "address",
                        "address_status",
                        "longitude",
                        "latitude",
                    ]
                )

                batch_number = 1
                while True:
                    rows = cursor.fetchmany(settings.etl.batch_size)
                    if not rows:
                        break

                    logger.info(f"Processing batch {batch_number}")
                    process_rows(rows, writer)

                    batch_number += 1


if __name__ == "__main__":
    start_time = time.time()

    main()

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Total execution time: {execution_time:.2f} seconds")
