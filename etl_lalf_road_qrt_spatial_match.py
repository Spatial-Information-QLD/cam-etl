import time
import multiprocessing
import logging
from concurrent.futures import ProcessPoolExecutor
from typing import List, Dict
from rich.progress import (
    Progress,
    SpinnerColumn,
    TextColumn,
    BarColumn,
    TaskProgressColumn,
    TimeRemainingColumn,
)

from psycopg import Cursor

from cam.etl import get_db_connection
from cam.etl.settings import settings
from cam.etl.types import Row

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("etl_lalf_road_qrt_spatial_match.log"),
    ],
)
logger = logging.getLogger(__name__)


def get_addresses(cursor: Cursor) -> list[Row]:
    """Get all address IDs from the database where the qrt_found value is null"""
    cursor.execute(
        """
        select a.addr_id, r.road_id, r.qrt_road_name_basic, r.qrt_road_id, l."lalf.locality_name"
        from "lalfpdba.lf_address" a
        join "lalfpdba.lf_road" r on a.road_id = r.road_id
        JOIN lalf_pndb_localities_joined l ON r.locality_code = l."lalf.locality_code"
        where
            a.addr_status_code != 'H'
            and r.qrt_found is null
        ;
    """
    )
    return list(cursor.fetchall())


def get_qrt_road(address_id: str, cursor: Cursor) -> list[Row]:
    """Query the database to find QRT road information for an address"""
    try:
        query = f"""
            WITH road_point AS (
                SELECT
                    r.qrt_road_name_basic,
                    (r.road_name || ' ' || r.road_name_type_code) as lf_road_name,
                    r.road_id as lf_road_id,
                    a.geom,
                    sp.centroid_lon,
                    sp.centroid_lat
                FROM "lalfpdba.lf_address" a
                JOIN "lalfpdba.lf_road" r ON a.road_id = r.road_id
                JOIN "lalfpdba.lf_geocode" g ON a.site_id = g.site_id
                JOIN "lalfpdba.sp_survey_point" sp ON g.spdb_pid = sp.pid
                WHERE
                    a.addr_id = '{address_id}'
                LIMIT 1
            )

            SELECT closest.road_name1, closest.road_id, rp.lf_road_name, rp.lf_road_id, rp.centroid_lon, rp.centroid_lat
            FROM (
                SELECT
                    q.road_name_,
                    q.road_id,
                    q.road_name1,
                    q.locality_l,
                    q.locality_r,
                    q.lga_name_l,
                    q.lga_name_r,
                    ST_Distance(q.geom, rp.geom) AS distance
                FROM qrt_spatial q
                CROSS JOIN road_point rp
                ORDER BY q.geom <-> rp.geom
                LIMIT 100
            ) as closest
            join road_point rp on closest.road_name1 = rp.qrt_road_name_basic
            limit 1;
        """
        cursor.execute(query)
        return cursor.fetchone()
    except Exception as e:
        logger.error(f"Error querying QRT road for address_id {address_id}: {e}")
        return None


def qrt_found_value(lalf_road_id: str, cursor: Cursor) -> bool | None:
    """Get the qrt_found value.

    true - found
    false - not found
    null - not yet processed
    """
    cursor.execute(
        """
        select qrt_found
        from "lalfpdba.lf_road"
        where road_id = %s;
        """,
        (lalf_road_id,),
    )
    result = cursor.fetchone()
    return result["qrt_found"]


def add_qrt_road_id_to_lalf_road(lalf_road_id: str, qrt_road_id: str, cursor: Cursor):
    """Add a QRT road id to a LALF road"""
    cursor.execute(
        """
        update "lalfpdba.lf_road"
        set qrt_road_id = %s, qrt_found = true
        where road_id = %s;
        """,
        (qrt_road_id, lalf_road_id),
    )


def add_qrt_found_value_to_lalf_road(
    lalf_road_id: str, qrt_found: bool, cursor: Cursor
):
    """Add a qrt_found value to a LALF road"""
    cursor.execute(
        """
        update "lalfpdba.lf_road"
        set qrt_found = %s
        where road_id = %s;
        """,
        (qrt_found, lalf_road_id),
    )


def process_address_chunk(
    address_chunk: List[Row], worker_id: int, counters: Dict
) -> None:
    """Process a chunk of addresses in a separate process"""
    try:
        with get_db_connection(
            host=settings.etl.db.host,
            port=settings.etl.db.port,
            dbname=settings.etl.db.name,
            user=settings.etl.db.user,
            password=settings.etl.db.password,
        ) as conn:
            with conn.cursor() as cursor:
                for address in address_chunk:
                    qrt_found = qrt_found_value(address["road_id"], cursor)
                    if qrt_found is None:
                        qrt_road = get_qrt_road(address["addr_id"], cursor)
                        if qrt_road:
                            add_qrt_road_id_to_lalf_road(
                                address["road_id"], qrt_road["road_id"], cursor
                            )
                        else:
                            add_qrt_found_value_to_lalf_road(
                                address["road_id"], False, cursor
                            )
                        conn.commit()

                    # Update the counter for this worker
                    counters[worker_id] += 1
    except Exception as e:
        logger.error(f"Error in worker process {worker_id}: {e}", exc_info=True)
        raise


def main():
    start_time = time.time()
    num_workers = 10

    try:
        with get_db_connection(
            host=settings.etl.db.host,
            port=settings.etl.db.port,
            dbname=settings.etl.db.name,
            user=settings.etl.db.user,
            password=settings.etl.db.password,
        ) as conn:
            with conn.cursor() as cursor:
                addresses = get_addresses(cursor)
                logger.info(f"Found {len(addresses)} addresses with no QRT road id")

                # Split addresses into chunks for each worker
                chunk_size = len(addresses) // num_workers
                if chunk_size == 0:
                    chunk_size = 1

                address_chunks = []
                for i in range(0, len(addresses), chunk_size):
                    if (
                        i + chunk_size > len(addresses)
                        or i // chunk_size >= num_workers - 1
                    ):
                        # Last chunk gets all remaining addresses
                        address_chunks.append(addresses[i:])
                        break
                    else:
                        address_chunks.append(addresses[i : i + chunk_size])

                # Create a Manager to hold shared counters between processes
                with multiprocessing.Manager() as manager:
                    # Create a dictionary to track progress for each worker
                    counters = manager.dict()
                    for i in range(num_workers):
                        counters[i] = 0

                    # Create progress bars
                    with Progress(
                        SpinnerColumn(),
                        TextColumn("[progress.description]{task.description}"),
                        BarColumn(),
                        TaskProgressColumn(),
                        TimeRemainingColumn(),
                    ) as progress:
                        # Create a task for each worker
                        tasks = [
                            progress.add_task(
                                f"[cyan]Worker {i+1} processing...", total=len(chunk)
                            )
                            for i, chunk in enumerate(address_chunks)
                        ]

                        # Start the workers
                        with ProcessPoolExecutor(max_workers=num_workers) as executor:
                            # Submit tasks to the executor
                            futures = [
                                executor.submit(
                                    process_address_chunk, chunk, i, counters
                                )
                                for i, chunk in enumerate(address_chunks)
                            ]

                            # Monitor progress while the workers are running
                            running = [True] * len(futures)
                            while any(running):
                                for i, future in enumerate(futures):
                                    if running[i]:
                                        if future.done():
                                            running[i] = False
                                            try:
                                                future.result()  # This will raise any exceptions from the worker
                                            except Exception as e:
                                                logger.error(f"Worker {i} failed: {e}")
                                                running[i] = False
                                        else:
                                            # Update the progress bar with the current count
                                            progress.update(
                                                tasks[i], completed=counters[i]
                                            )
                                time.sleep(0.1)  # Small delay to avoid busy waiting

    except Exception as e:
        logger.error(f"Error in main process: {e}", exc_info=True)
    finally:
        end_time = time.time()
        execution_time = end_time - start_time
        logger.info(f"Total execution time: {execution_time:.2f} seconds")


if __name__ == "__main__":
    main()
