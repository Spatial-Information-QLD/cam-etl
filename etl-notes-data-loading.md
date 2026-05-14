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

Load the vocab data that's been tested and used in QALI UAT.

```sh
curl -X POST \
    -H "Content-Type: application/rdf-patch" \
    --data-binary @vocabs.rdfp \
    http://localhost:3030/ds/patch
```

## Transfer

Stop local Fuseki and package the generated local Fuseki data for transfer.

This zips `fuseki-data/`, including:

- `fuseki-data/databases/ds`
- `fuseki-data/run/databases/ds_lucene_index`

The zip also contains local config and runtime directories, but the remote copy steps below only install the generated TDB2 database and generated Lucene index. That preserves the remote Fuseki configuration and security files.

This results in a `fuseki-data.zip` file.

```sh
task fuseki:qali:tdb2:zip
```

Transfer to the remote server using rsync.

```sh
# ~1 hour 15 minutes
time rsync -avz --progress fuseki-data.zip cam-itp-dev-fuseki:/data
```

If the transfer is interrupted, you can resume it using the `-P` flag.

Unzip the file into a staging directory on the remote server.

```sh
mkdir -p /data/fuseki-data-transfer
cd /data/fuseki-data-transfer
unzip /data/fuseki-data.zip
```

Stop the database.

```sh
sudo systemctl stop container-fuseki.service
```

Delete the old generated database and generated full-text index.

```sh
sudo rm -rf /data/fuseki-data/databases/ds
sudo rm -rf /data/fuseki-data/run/databases/ds_lucene_index
```

Copy the generated database and generated full-text index into the Fuseki data directory.

```sh
sudo mkdir -p /data/fuseki-data/databases
sudo mkdir -p /data/fuseki-data/run/databases
sudo cp -R /data/fuseki-data-transfer/fuseki-data/databases/ds /data/fuseki-data/databases/ds
sudo cp -R /data/fuseki-data-transfer/fuseki-data/run/databases/ds_lucene_index /data/fuseki-data/run/databases/ds_lucene_index
```

This copies the data generated locally while leaving the remote `fuseki-data/configuration`, `shiro.ini`, logs, backups, templates, and system files unchanged.

If the Fuseki service expects a specific owner or group for `/data/fuseki-data`, set ownership to match the existing directory before starting Fuseki.

```sh
ls -ld /data/fuseki-data
ls -ld /data/fuseki-data/databases/ds
ls -ld /data/fuseki-data/run/databases/ds_lucene_index
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
