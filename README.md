# CAM ETL

## Place names

The place names source data is from PNDB. A dump of the files are in `pndb/`.

A set of the relevant tables are documented below.

### Place name type

File: [pndb/lapnpdba.pntypes.csv](pndb/lapnpdba.pntypes.csv)

This look up table contains the type of place names and should align with the [Place Names Categories from ICSM](http://icsm.surroundaustralia.com/object?uri=https%3A//linked.data.gov.au/def/placenames-categories).

The terms from the look up table not found in ICSM Place Names Categories:

```python
['anchorage', 'bank - marine', 'bar', 'bay', 'beach', 'bore', 'breakwater', 'cape', 'cave', 'cay', 'channel', 'cliff', 'corner', 'county', 'cove, inlet', 'crater', 'dam wall', 'desert', 'district', 'drain', 'dune', 'entrance', 'ford', 'forest', 'gate', 'gorge', 'gulf', 'harbour', 'hill', 'historic site', 'homestead', 'ignore - test record', 'inlet', 'island', 'island - feature appears absent', 'island group', 'isthmus', 'junction', 'lagoon', 'lake', 'landing area', 'landing place', 'locality bounded', 'locality unbounded', 'lookout', 'marine', 'mountain', 'mountain - feature no longer exists', 'national park,resources reserve,conservation park', 'neighbourhood', 'outstation', 'pan', 'parish', 'park', 'pass', 'passage', 'pastoral district', 'peak', 'peak - feature no longer exists', 'peninsula', 'place name', 'plain', 'plateau', 'plateau - marine', 'pocket', 'point', 'population centre', 'population centre - feature appears absent', 'port', 'rail station', 'rail station - feature no longer exists', 'range', 'rapids', 'reach', 'reef', 'reserve', 'reservoir', 'ridge', 'ridge - marine', 'rock', 'rockhole', 'school', 'scrub', 'shelf - marine', 'shoal', 'siding', 'soak', 'sound', 'spit', 'spring', 'spur', 'state', 'state forest', 'stockyard', 'strait', 'suburb', 'valley', 'water tank', 'watercourse', 'waterfall', 'waterhole', 'weir', 'well', 'wetland']
```
