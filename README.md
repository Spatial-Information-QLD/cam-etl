# CAM ETL

## Getting started

With Docker Desktop running with at least 8 GB of memory allocated, start GraphDB.

```bash
task graphdb:up
```

Go to http://localhost:7200/repository and create a new GraphDB repository named `addressing` as the repository ID. Tick `Enable SHACL validation` and click `Create`.

Now that the repository is created, stop GraphDB.

```bash
task graphdb:down
```

Download the Addressing RDF data from <sharepoint-url>, unzip it in the root of this project directory and ensure the directory containing the `*.nq` files is named `output`. This directory gets mounted into GraphDB `preload` service as defined in the [docker-compose.yml](docker-compose.yml).

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

## ETL

Bulk loading N-Quads CAM data into GraphDB takes around 16 minutes.

## Postcodes

Before loading, see if PostGIS is enabled for the `lalfdb` schema.

```sql
SHOW search_path;
-- If not available, set it.
SET search_path = public, lalfdb;
```

Load the `QLD_POSTCODE.dbf` file into Postgres.

```bash
shp2pgsql -D -I -s 4326 "/tmp/postgres-data/postcodes/Postcode Boundaries/Postcode Boundaries MAY 2023/Standard/QLD_POSTCODE.dbf" lalfdb.postcode | psql address postgres
# shp2pgsql -D -I -s 4326 "/tmp/postgres-data/postcodes/Postcode Boundaries/Postcode Boundaries MAY 2023/Standard/QLD_POSTCODE_POLYGON.shp" lalfdb.postcode_polygon | psql address postgres
```

Note, I was not able to load the `QLD_POSTCODE_POLYGON.shp` file using `shp2pgsql`. I had to open it in QGIS and export it as CSV. I then used DBeaver to load the CSV into the database.

## QRT Roads

Download from https://qldspatial.information.qld.gov.au/catalogue/custom/detail.page?fid={CE66D3D5-8740-41A7-8B42-30F5F1691B36}.

The data was converted from a GeoDatabase to CSV using QGIS and loaded in as a table.

The GeoDatabase supplied by Anne had historical data included. The Shapefile downloaded directly from the link above has the correct data for the ETL.

To be able to get some of the roads data from the addressing database and join it together with QRT, add a `qrt_road_name_basic` with values concatenating from `lf_road`'s `road_name` and `lf_road_name_type.road_name_type`.

```sql
ALTER TABLE lalfdb.lalfpdba_lf_road
	ADD COLUMN qrt_road_name_basic VARCHAR(255);

UPDATE
	lalfdb.lalfpdba_lf_road r
SET
	qrt_road_name_basic = r.road_name || ' ' || rnt.road_name_type
FROM lalfdb.lalfpdba_lf_road_name_type rnt
WHERE rnt.road_name_type_code = r.road_name_type_code;
```

We also need to align the locality values in QRT and the Addressing database's locality table. To do this, convert the QRT's `locality_left` column's value to an uppercase and insert it into a new column named `address_locality`.

```sql
ALTER TABLE lalfdb.qrt
	ADD COLUMN address_locality VARCHAR(255);

UPDATE
	lalfdb.qrt q
SET
	address_locality = UPPER(q.locality_left);
```

````

## Addressing DB

See the schema documentation here: https://spatial-information-qld.github.io/cam-etl/addressdb/

The dataset is several GBs zipped. We may put a subset of it in this repository in the future for demo purposes.

### Tables

A bunch of tables were loaded in to a PostgreSQL database and a schema was created from the documentation provided.

#### lalfpdba_lf_address

The empty strings in the columns `level_type_code` and `unit_type_code` were converted to `NULL`.

The column `geocode_id` was added as a foreign key to the `lalfpdba_lf_geocode` table.

#### lalfpdba_sp_survey_point

The column `wkt_literal` was added with values derived from the existing columns `centroid_lon` and `centroid_lat`.

#### lalfpdba_lf_road

The column `locality_code` is a foreign key to the `locality` table. Can't actually create it though since data is not correct (e.g., some data missing).

## Place names

The place names source data is from PNDB. A dump of the files are in `pndb/`.

Here is the schema of the tables of interest.

![pndb/pndb-schema.png](pndb/pndb-schema.png)

A set of the relevant tables are documented below.

### Place name type

File: [pndb/lapnpdba.pntypes.csv](pndb/lapnpdba.pntypes.csv)

This look up table contains the type of place names and should align with the [Place Names Categories from ICSM](http://icsm.surroundaustralia.com/object?uri=https%3A//linked.data.gov.au/def/placenames-categories).

The terms from the look up table not found in ICSM Place Names Categories:

<details>
    <summary>View the missing terms</summary>

    ```python
    [
        'Anchorage',
        'Bank - Marine',
        'Bar',
        'Bay',
        'Beach',
        'Bore',
        'Breakwater',
        'Cape',
        'Cave',
        'Cay',
        'Channel',
        'Cliff',
        'Corner',
        'County',
        'Cove, Inlet',
        'Crater',
        'Dam wall',
        'Desert',
        'District',
        'Drain',
        'Dune',
        'Entrance',
        'Ford',
        'Forest',
        'Gate',
        'Gorge',
        'Gulf',
        'Harbour',
        'Hill',
        'Historic Site',
        'Homestead',
        'Inlet',
        'Island',
        'Island - feature appears absent',
        'Island group',
        'Isthmus',
        'Junction',
        'Lagoon',
        'Lake',
        'Landing Area',
        'Landing Place',
        'Locality Bounded',
        'Locality Unbounded',
        'Lookout',
        'Marine',
        'Mountain',
        'Mountain - Feature no longer exists',
        'National Park,Resources Reserve,Conservation Park',
        'Neighbourhood',
        'Outstation',
        'Pan',
        'Parish',
        'Park',
        'Pass',
        'Passage',
        'Pastoral district',
        'Peak',
        'Peak - Feature no longer exists',
        'Peninsula',
        'Place Name',
        'Plain',
        'Plateau',
        'Plateau - Marine',
        'Pocket',
        'Point',
        'Population centre',
        'Population centre - feature appears absent',
        'Port',
        'Rail Station',
        'Rail Station - Feature no longer exists',
        'Range',
        'Rapids',
        'Reach',
        'Reef',
        'Reserve',
        'Reservoir',
        'Ridge',
        'Ridge - Marine',
        'Rock',
        'Rockhole',
        'School',
        'Scrub',
        'Shelf - Marine',
        'Shoal',
        'Siding',
        'Soak',
        'Sound',
        'Spit',
        'Spring',
        'Spur',
        'State',
        'State Forest',
        'Stockyard',
        'Strait',
        'Suburb',
        'Valley',
        'Water tank',
        'Watercourse',
        'Waterfall',
        'Waterhole',
        'Weir',
        'Well',
        'Wetland',
        'ignore - test record'
    ]
    ```

</details>
````
