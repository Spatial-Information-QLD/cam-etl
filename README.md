# CAM ETL

## Addressing DB

See the schema documentation here: https://spatial-information-qld.github.io/cam-etl/addressdb/

The dataset is several GBs zipped. We may put a subset of it in this repository in the future for demo purposes.

## Place names

The place names source data is from PNDB. A dump of the files are in `pndb/`.

A set of the relevant tables are documented below.

### Place name type

File: [pndb/lapnpdba.pntypes.csv](pndb/lapnpdba.pntypes.csv)

This look up table contains the type of place names and should align with the [Place Names Categories from ICSM](http://icsm.surroundaustralia.com/object?uri=https%3A//linked.data.gov.au/def/placenames-categories).

The terms from the look up table not found in ICSM Place Names Categories:

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
