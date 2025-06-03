# Data Loading

## Create the TDB2 Database

Copy the RDF data to the Fuseki VM.

```sh
scp 2025-05-29.data.zip cam-itp-dev-fuseki:/data
```

Unzip the zipped file.

```sh
unzip 2025-05-29.data.zip
```

Create the TDB2 database using xloader. The unzipped folder on the host should be named `graphdb-import`. The target TDB2 database folder is named `new-fuseki-data` on the host. We mount both these folders into the container.

```sh
sudo podman run --rm -it -v ./graphdb-import:/home/graphdb-import -v ./new-fuseki-data:/home/fuseki-data ghcr.io/kurrawong/fuseki:5.2.0-0 /bin/bash -c 'tdb2.xloader --threads 2 --loc /home/fuseki-data/qali /home/graphdb-import/*.nq'
```

## QALI Data

In the `/data` directory, stop the database.

```sh
sudo systemctl stop container-fuseki.service
```

Mount the volumes into the new container and create the TDB2 database using xloader on the n-quads files.

```sh
sudo podman run --rm -it -v ./graphdb-import:/home/graphdb-import -v ./new-fuseki-data:/home/fuseki-data ghcr.io/kurrawong/fuseki:5.2.0-0 /bin/bash -c 'tdb2.xloader --threads 2 --loc /home/fuseki-data/qali /home/graphdb-import/*.nq'
```

Running it with docker instead.

```sh
time docker run --rm -it -v ./graphdb-import:/home/graphdb-import -v ./new-fuseki-data:/home/fuseki-data ghcr.io/kurrawong/fuseki:5.2.0-0 /bin/bash -c 'tdb2.xloader --threads 10 --loc /home/fuseki-data/qali /home/graphdb-import/*.nq'
```

## Transfer

Use `zip` to create a single compressed file for network transfer.

```sh
# ~15 minutes
time zip -r new-fuseki-data.zip new-fuseki-data/
```

Transfer to the remote server using rsync.

```sh
# ~1 hour 15 minutes
time rsync -avz --progress new-fuseki-data.zip cam-itp-dev-fuseki:/data
```

If the transfer is interrupted, you can resume it using the `-P` flag.

## Fuseki Full-Text Indexing

We need to stop the database to release the file lock on the full-text index on disk.

```sh
sudo systemctl stop container-fuseki.service
```

Now run the full-text indexer. Notice that we volume mount the data directory and the `config.ttl` into the container.

```sh
sudo podman run --rm -v /data/fuseki-data:/fuseki -v /etc/fuseki/config.ttl:/opt/rdf-delta/config.ttl ghcr.io/kurrawong/rdf-delta:0.1.12 /bin/bash -c 'java -cp rdf-delta-fuseki-server.jar:compoundnaming.jar jena.textindexer --desc=config.ttl'
```

Start the database after indexing is complete.

```sh
sudo systemctl start container-fuseki.service
```

## Loading Auxiliary Data

Install `uv`.
Using `uv`, install `kurra`.

```sh
uv tool install kurra
```

### Loading Vocab Data

```sh
kurra db upload vocabs-import/ http://localhost:3030/ds
```

### Loading User Data

Create a `users.trig` file and upload it **using curl**. The reason why we use curl is, this is a trig file and we need the user info to go into a specific graph that QALI understands. Populate the file based off of the example in github.com/kurrawong/cam.

```sh
curl -X POST http://localhost:3030/ds -H "Content-Type: text/trig" --data-binary @./users.trig

```
