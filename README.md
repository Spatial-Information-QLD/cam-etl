# CAM ETL

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
