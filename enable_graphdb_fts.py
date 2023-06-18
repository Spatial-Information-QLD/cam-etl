import time

from cam.graphdb import autocomplete


def main() -> None:
    graphdb_url = "http://localhost:7200"
    repository_id = "addressing"
    autocomplete(graphdb_url, repository_id, True)


if __name__ == "__main__":
    starttime = time.time()

    try:
        main()
    finally:
        endtime = time.time() - starttime
        print(f"Completed in {endtime:0.2f} seconds")
