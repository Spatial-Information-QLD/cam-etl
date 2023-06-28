# CAM ETL

## Getting started

The notes below is to get an instance of GraphDB running with the output of the ETL data loaded and a simple web application querying GraphDB. For instructions on running the ETL, see [etl-notes.md](etl-notes.md).

With Docker Desktop running with at least 8 GB of memory allocated, start GraphDB.

Note that the following commands prefixed with `task` requires the [Taskfile CLI tool installed](https://taskfile.dev/installation/).

```bash
task graphdb:up
```

Go to http://localhost:7200/repository and create a new GraphDB repository named `addressing` as the repository ID. Tick `Enable SHACL validation` and click `Create`.

Now that the repository is created, stop GraphDB.

```bash
task graphdb:down
```

Download the Addressing RDF data from this [SharePoint](https://itpqld.sharepoint.com.mcas.ms/sites/R-SICAMProjectBoard/Shared%20Documents/Forms/AllItems.aspx?id=%2Fsites%2FR%2DSICAMProjectBoard%2FShared%20Documents%2FGeneral%2FSandbox%20ETL&viewid=d8225c45%2D5e3a%2D4dda%2Db296%2Db01e4ae1eb77) folder, unzip it in the root of this project directory and ensure the directory containing the `*.nq` files is named `output`. This directory gets mounted into GraphDB `preload` service as defined in the [docker-compose.yml](docker-compose.yml).

Bulk load the data using the [GraphDB ImportRDF tool](https://graphdb.ontotext.com/documentation/10.2/loading-data-using-importrdf.html) with the preload option.

```bash
task graphdb:preload
```

This should take around 19 minutes.

Start up GraphDB again.

```bash
task graphdb:up
```

With Python 3.10 or higher installed, create a Python virtual environment, activate it, and install the Python dependencies.

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Enable the autocomplete index in GraphDB.

```bash
python enable_graphdb_fts.py
```

The autocomplete index should now be building. The whole building process should take around 10 minutes. See http://localhost:7200/autocomplete for its status.

Once the autocomplete index completes building, head to http://localhost:7200 and try and search for an address in the `View resource` search box.

## Web UI Demo

A simple web user-interface can be started using the following command.

```bash
task web:up
```

This application is a demonstration of how a simple downstream application can interface with the graph database. Users can search for addresses using the full-text search functionality and view each address with the full address name in a templated format, a graph view of it in GraphDB, and the raw RDF data view in the Turtle format.

## Loading SHACL shapes

Go to http://localhost:7200/import#user and load `shacl.ttl` into the graph `<http://rdf4j.org/schema/rdf4j#SHACLShapeGraph>`.

To test the SHACL validator, go to http://localhost:7200/import# and use Import RDF text snippet. Paste the data below into the text field.

```turtle
PREFIX addr: <https://w3id.org/profile/anz-address/>

<urn:example:1> a addr:Address .
```

Once import is completed, an error will appear as

```
org.eclipse.rdf4j.sail.shacl.GraphDBShaclSailValidationException: Failed SHACL validation
```

## Postgres Backup and Restore

The backup and restore process documented here uses `pg_dump`. See [Postgres' backup-dump.html](https://www.postgresql.org/docs/current/backup-dump.html) for more information.

### Backup

A Postgres data dump of the Addressing database was performed using `pg_dump`.

The `--password` prompts for the password.

```
pg_dump --username postgres --password -d address > /tmp/postgres-backup/address
```

### Restore

To restore, download the `postres-backup.zip` file from this [SharePoint](https://itpqld.sharepoint.com.mcas.ms/sites/R-SICAMProjectBoard/Shared%20Documents/Forms/AllItems.aspx?id=%2Fsites%2FR%2DSICAMProjectBoard%2FShared%20Documents%2FGeneral%2FSandbox%20ETL&viewid=d8225c45%2D5e3a%2D4dda%2Db296%2Db01e4ae1eb77) folder.

Run `psql` to restore the database.

Create the database named `address`.

```
echo "CREATE DATABASE address;" | psql --username postgres --password -d postgres
```

Restore the data to the newly created `address` database.

```
psql --username postgres --password -d address < /tmp/postgres-backup/address
```
