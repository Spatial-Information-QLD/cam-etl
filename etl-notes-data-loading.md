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

Unzip the file on the remote server.

```sh
unzip new-fuseki-data.zip
```

Stop the database.

```sh
sudo systemctl stop container-fuseki.service
```

Delete the old database.

```sh
sudo rm -rf /data/fuseki-data/databases/ds
```

Move the unzipped data to the fuseki data directory.

```sh
mv new-fuseki-data/qali /data/fuseki-data/databases/ds
```

Run the full-text indexer.

```sh
sudo podman run --rm -v /data/fuseki-data:/fuseki -v /etc/fuseki/config.ttl:/opt/rdf-delta/config.ttl ghcr.io/kurrawong/rdf-delta:0.1.12 /bin/bash -c 'java -cp rdf-delta-fuseki-server.jar:compoundnaming.jar jena.textindexer --desc=config.ttl'
```

Start the database.

```sh
sudo systemctl start container-fuseki.service
```

Test that the full-text index is working.

```sh
curl -X POST http://localhost:3030/ds -H "Content-Type: application/sparql-query" --data 'SELECT * WHERE { GRAPH <urn:qali:graph:addresses> { (?iri ?score ?value) <http://jena.apache.org/text#query> (<https://schema.org/identifier> "SP11950*" 1000) . } } ORDER BY DESC(?score) LIMIT 10' | jq
```

## Loading Auxiliary Data

### Loading Vocab Data

In the cam-etl repo, run the following with curl to load the vocabs into a named graph.

```sh
for f in vocabs-import/*.ttl; do
    echo "Uploading $f"
    curl -X POST -H "Content-Type: text/turtle" --data-binary @$f 'http://localhost:3030/ds?graph=urn:qali:graph:vocabs'
done
```

### Loading User Data

Create a `users.trig` file and upload it **using curl**. The reason why we use curl is, this is a trig file and we need the user info to go into a specific graph that QALI understands. Populate the file based off of the example in github.com/kurrawong/cam.

```sh
curl -X POST http://localhost:3030/ds -H "Content-Type: text/trig" --data-binary @./users.trig

```

### Test the Compound Naming Function

```sh
curl -X POST http://localhost:3030/ds \
  -H "Content-Type: application/sparql-query" \
  --data 'PREFIX addr: <https://linked.data.gov.au/def/addr/>
PREFIX cn: <https://linked.data.gov.au/def/cn/>
PREFIX cnf: <https://linked.data.gov.au/def/cn/func/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX sdo: <https://schema.org/>
PREFIX text: <http://jena.apache.org/text#>

SELECT ?address ?partIds ?partTypes ?partValuePredicate ?partValue
WHERE {
  GRAPH <urn:qali:graph:addresses> {
    {
      SELECT ?address
      WHERE {
        ?address a addr:Address
      }
      limit 1
    }
    ?address cnf:getParts (?partIds ?partTypes ?partValuePredicate ?partValue) .
  }
}' | jq
```
