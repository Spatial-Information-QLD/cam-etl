# Running the ETL

The ETL created from 2023 in the CAM1 project used pyspark where the entire table was loaded into memory and distributed to workers. In the CAM3 2024 project, data is now loaded into a local `lalfdb` postgres database in the default `public` schema, and pyspark is no longer used. Rather than dealing with the complexities of pyspark and the overhead of setting up the Java dependency, python multiprocessing and the psycopg library with server-side cursor is used instead. This allows for better performance, less overhead and easier memory pressure management.

## Setting things up

Start the postgres database service.

```sh
task postgres:up
```

Create the `lalfdb` database via `psql` by running the following commands in the postgres container.

```sh
psql -d postgres -c "CREATE DATABASE lalfdb;" -U postgres -w
psql -d lalfdb -c "CREATE EXTENSION postgis;" -U postgres -w
```

## QRT - Queensland Roads and Tracks

Download QRT v2, a new dataset schema created by Anne Goldsack from [R-SI CAM Project Board > General > Stage 3 - Location Addressing Rollout > Legacy DB exports](https://itpqld.sharepoint.com/sites/R-SICAMProjectBoard/Shared%20Documents/Forms/AllItems.aspx?id=%2Fsites%2FR-SICAMProjectBoard%2FShared%20Documents%2FGeneral%2FStage%203%20-%20Location%20Addressing%20Rollout%2FLegacy%20DB%20exports).

Use a postgres client to load the flat QRT CSV file into the `lalfdb` database into the `qrt` table. Map all columns to the `text` data type.

To run the ETL, run the following command.

```sh
task etl:db:qrt
```

## Place Names Database

The Place Names Database contains both gazetted and non-gazetted place names.

Download the Place Names Database from [R-SI CAM Project Board > General > Stage 3 - Location Addressing Rollout > Legacy DB exports](https://itpqld.sharepoint.com/:u:/r/sites/R-SICAMProjectBoard/Shared%20Documents/General/Stage%203%20-%20Location%20Addressing%20Rollout/Legacy%20DB%20exports/PNDB.zip?csf=1&web=1&e=3C8MkR).

Use a postgres client to load the PNDB CSV files into the `lalfdb` database. Map the CSV file to the following table names with columns mapped to the `text` data type.

- `Tags.csv` -> `pndb.tags`
- `Place name.csv` -> `pndb.place_name`
- `Indigenous name.csv` -> `pndb.indigenous_name`
- `History.csv` -> `pndb.history`

The `pndb.place_name.type_resolved` column maps to the [Geographical Names Categories vocabulary](https://github.com/geological-survey-of-queensland/vocabularies/blob/b07763c87f2f872133197e6fb0eb911de85879c6/vocabularies-qsi/go-categories.ttl).

To run the ETL, run the following command.

```sh
task etl:db:pndb
```

## LALF

The LALF is the Ingress Queensland addressing database. It contains tables that are necessary to form a valid Queensland address object.

The following files contain null terminator characters, which are not supported in PostgreSQL. Run the `addressdb/remove_null_terminator_char.py` script to remove the null terminator characters from the files.

- `lalfpdba.lf_incremental_action.csv`
- `lalfpdba.lf_address.csv`
- `lalfpdba.lf_address_history.csv`

### Place Names

The place names data in the LALF is separate to the PNDB. These place names are used mainly from an addressing perspective and includes names for things like properties, buildings, etc.

---

## Addressing Database

- `lf_address_history`

## ETL Data Loading Notes

Below are some notes on loading the addressing database dump, place names data dump and QRT data dump into a PostgreSQL database with the PostGIS extension.

### Postcodes

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

### QRT Roads

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

### Addressing DB

See the schema documentation here: https://spatial-information-qld.github.io/cam-etl/addressdb/

The dataset is several GBs zipped. We may put a subset of it in this repository in the future for demo purposes.

#### Tables

A bunch of tables were loaded in to a PostgreSQL database and a schema was created from the documentation provided.

##### lalfpdba_lf_address

The empty strings in the columns `level_type_code` and `unit_type_code` were converted to `NULL`.

The column `geocode_id` was added as a foreign key to the `lalfpdba_lf_geocode` table.

##### lalfpdba_sp_survey_point

The column `wkt_literal` was added with values derived from the existing columns `centroid_lon` and `centroid_lat`.

##### lalfpdba_lf_road

The column `locality_code` is a foreign key to the `locality` table. Can't actually create it though since data is not correct (e.g., some data missing).

### Place names

The place names source data is from PNDB. A dump of the files are in `pndb/`.

Here is the schema of the tables of interest.

![pndb/pndb-schema.png](pndb/pndb-schema.png)

A set of the relevant tables are documented below.

#### Place name type

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
