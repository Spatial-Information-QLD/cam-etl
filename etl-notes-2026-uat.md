## Parcels

Parcel count for both queries: 3,332,046

```sparql
PREFIX addr: <https://linked.data.gov.au/def/addr/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX roads: <https://linked.data.gov.au/def/roads/>
SELECT (count(?addr_obj) as ?count)
FROM <urn:qali:graph:addresses>
WHERE {
  ?addr_obj a addr:AddressableObject
} LIMIT 10
```

```sql
SELECT count(*)
FROM (
  SELECT
      p.parcel_id,
      CASE
          WHEN
              p.lot_no = '9999' AND p.plan_no NOT IN ('SP292760', 'SP304737', 'SP288122', 'SP260341', 'SP245185', 'SP271925')
          THEN
              '0'
          ELSE
              p.lot_no
      END AS lot_no,
      p.plan_no,
      p.parcel_status_code,
      p.parcel_create_date,
      p.parcel_org_source_code,
      p.parcel_data_source_code,
      p.parcel_data_source_date
  FROM
      "lalfpdba.lf_parcel" p
  WHERE p.parcel_status_code != 'D'
) AS count;
```

## Addresses

Address count for both queries: 2,830,543

```sparql
PREFIX addr: <https://linked.data.gov.au/def/addr/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT (count(?addr) as ?count)
FROM <urn:qali:graph:addresses>
WHERE {
  ?addr a addr:Address
} LIMIT 10
```

```sql
select count(*)
from "lalfpdba.lf_address" a
where a.addr_status_code != 'H';
```

## Roads

The road names SPARQL query returned 163,966 results.

```sparql
PREFIX addr: <https://linked.data.gov.au/def/addr/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX roads: <https://linked.data.gov.au/def/roads/>
SELECT (count(?road) as ?count)
FROM <urn:qali:graph:roads>
WHERE {
  ?road a roads:RoadName
} LIMIT 10
```

The road objects SPARQL query returned 163,966 results.

```sparql
PREFIX addr: <https://linked.data.gov.au/def/addr/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX roads: <https://linked.data.gov.au/def/roads/>
SELECT (count(?road_obj) as ?count)
FROM <urn:qali:graph:roads>
WHERE {
  ?road_obj a roads:RoadObject
} LIMIT 10
```

The SQL query returned 158,264 results.

```sql
SELECT COUNT(*)
FROM (
    SELECT DISTINCT
        q.road_id,
        q.road_name_,
        q.road_name,
        q.road_type,
        q.road_suffi
    FROM qrt_spatial q
) AS unique_roads;
```

The number of LALF roads that were not matched against a road object in QRT.

Count: 5744

```sparql
PREFIX sdo: <https://schema.org/>
PREFIX addr: <https://linked.data.gov.au/def/addr/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX roads: <https://linked.data.gov.au/def/roads/>
SELECT ?identifier
FROM <urn:qali:graph:roads>
WHERE {
  ?road_obj a roads:RoadObject ;
  sdo:identifier ?identifier
  FILTER(!STRSTARTS(STR(?identifier), "QLDR"))
}
```

The following query should not return any results.

```sparql
PREFIX apt: <https://linked.data.gov.au/def/addr-part-types/>
PREFIX addr: <https://linked.data.gov.au/def/addr/>
PREFIX cn: <https://linked.data.gov.au/def/cn/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX roads: <https://linked.data.gov.au/def/roads/>
PREFIX sdo: <https://schema.org/>
SELECT ?road (count(?name) as ?count)
FROM <urn:qali:graph:roads>
FROM <urn:qali:graph:addresses>
WHERE {
  ?road a roads:RoadObject ;
  cn:hasName ?road_name .
  ?road_name sdo:name ?name
}
group by ?road having (?count > 1)
```
