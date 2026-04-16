# Data Loading

## Create the TDB2 Database

The raw n-quads should be present in `fuseki-import/`.

Create the standalone TDB2 export locally.

It takes roughly 45 minutes to create the TDB2 database from the n-quads.

```sh
task fuseki:qali:tdb2:create
```

This creates the database in `new-fuseki-data/databases/ds`.

## QALI Data

Install the generated TDB2 database into `./fuseki-data` for local testing and rebuild the text index.

It takes around 10 minutes to install the TDB2 database and rebuild the text index.

```sh
task fuseki:qali:tdb2:install-local
```

Start Fuseki locally.

```sh
task fuseki:up
```

The local dataset is served at `http://localhost:3030/ds`.

You can test that Fuseki is running with:

```sh
curl http://localhost:3030/$/ping
```

## Transfer

Package the standalone TDB2 export for transfer.

This takes around 13 minutes and results in a `new-fuseki-data.zip` file of around 36GB.

```sh
task fuseki:qali:tdb2:zip
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

Move the unzipped data to the Fuseki data directory.

```sh
mv new-fuseki-data/databases/ds /data/fuseki-data/databases/ds
```

Run the full-text indexer using the Fuseki image and the dataset config in `/data/fuseki-data/configuration/ds.ttl`.

Note: this is not necessary if you ran task `fuseki:qali:tdb2:install-local` before zipping the data, as the text index is included in the TDB2 export. However, if you want to be sure the text index is up to date, you can run this command to rebuild it on the remote server after transferring the data.

```sh
sudo podman run --rm -v /data/fuseki-data:/fuseki ghcr.io/kurrawong/fuseki:5.6.0-0 /bin/bash -c 'rm -rf /fuseki/run/databases/ds_lucene_index && java -cp $FUSEKI_HOME/fuseki-server.jar:$FUSEKI_HOME/lib/* jena.textindexer --desc=/fuseki/configuration/ds.ttl'
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

Note: this is not necessary if the vocabs were included in the TDB2 export, but if you want to be sure the vocabs are up to date, you can run this command to load them after transferring the data.

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
