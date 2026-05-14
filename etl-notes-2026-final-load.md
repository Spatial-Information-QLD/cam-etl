# 2026 Final Load Validation Notes

Date: 2026-05-13

## Summary

The final Fuseki data reconciles with the source data loaded in Postgres for the main ETL outputs checked:

- Addresses: `2,833,300` current/non-historical addresses in Postgres and Fuseki.
- Road objects and road names: `164,249` in Fuseki, matching `158,532` distinct QRT road IDs plus `5,717` LALF-only missing-QRT roads.
- Parcels: `3,333,503` non-deleted parcels in Postgres and Fuseki.
- Address-linked parcels: `2,627,958` in Postgres and Fuseki.
- Address statuses in Fuseki match Postgres exactly.
- Road names, addresses, and geographical names each have exactly one `sdo:name` value.
- Expected graphs are present: addresses, roads, geographical names, vocabs, users, system, and tags.

## Postgres Checks

### Core Source Counts

```sql
SELECT 'qrt_spatial' AS source, count(*)::bigint AS rows
FROM public.qrt_spatial
UNION ALL
SELECT 'lf_address_total', count(*)::bigint
FROM public."lalfpdba.lf_address"
UNION ALL
SELECT 'lf_address_current_A_P', count(*)::bigint
FROM public."lalfpdba.lf_address"
WHERE addr_status_code IN ('A', 'P')
UNION ALL
SELECT 'lf_road_total', count(*)::bigint
FROM public."lalfpdba.lf_road"
UNION ALL
SELECT 'lf_road_current_P_X', count(*)::bigint
FROM public."lalfpdba.lf_road"
WHERE road_status_code IN ('P', 'X')
UNION ALL
SELECT 'lf_road_current_qt_null', count(*)::bigint
FROM public."lalfpdba.lf_road"
WHERE road_status_code IN ('P', 'X') AND qrt_found IS NULL
UNION ALL
SELECT 'lf_road_current_qt_true', count(*)::bigint
FROM public."lalfpdba.lf_road"
WHERE road_status_code IN ('P', 'X') AND qrt_found IS TRUE
UNION ALL
SELECT 'lf_road_current_qt_false', count(*)::bigint
FROM public."lalfpdba.lf_road"
WHERE road_status_code IN ('P', 'X') AND qrt_found IS FALSE;
```

Results:

| Source | Rows |
| --- | ---: |
| `qrt_spatial` | `557,478` |
| `lf_address_total` | `3,294,769` |
| `lf_address_current_A_P` | `2,833,185` |
| `lf_road_total` | `281,727` |
| `lf_road_current_P_X` | `123,754` |
| `lf_road_current_qt_null` | `6,470` |
| `lf_road_current_qt_true` | `111,567` |
| `lf_road_current_qt_false` | `5,717` |

### Address And Road Status Counts

```sql
SELECT 'address_status' AS type, addr_status_code AS code, count(*)::bigint AS rows
FROM public."lalfpdba.lf_address"
GROUP BY addr_status_code
UNION ALL
SELECT 'road_status', road_status_code, count(*)::bigint
FROM public."lalfpdba.lf_road"
GROUP BY road_status_code
ORDER BY type, code;
```

Results:

| Type | Code | Rows |
| --- | --- | ---: |
| `address_status` | `A` | `95,453` |
| `address_status` | `H` | `461,469` |
| `address_status` | `P` | `2,737,732` |
| `address_status` | `X` | `115` |
| `road_status` | `H` | `157,973` |
| `road_status` | `P` | `123,695` |
| `road_status` | `X` | `59` |

### Road Reconciliation Inputs

```sql
SELECT 'qrt_rows' AS metric, count(*)::bigint AS rows
FROM public.qrt_spatial
UNION ALL
SELECT 'qrt_distinct_road_id', count(DISTINCT road_id)::bigint
FROM public.qrt_spatial
UNION ALL
SELECT 'qrt_distinct_nonblank_road_id', count(DISTINCT road_id)::bigint
FROM public.qrt_spatial
WHERE road_id IS NOT NULL AND road_id <> ''
UNION ALL
SELECT 'lalf_roads_qt_false', count(*)::bigint
FROM public."lalfpdba.lf_road"
WHERE qrt_found IS FALSE
UNION ALL
SELECT 'lalf_roads_qt_true', count(*)::bigint
FROM public."lalfpdba.lf_road"
WHERE qrt_found IS TRUE
UNION ALL
SELECT 'lalf_roads_qt_null', count(*)::bigint
FROM public."lalfpdba.lf_road"
WHERE qrt_found IS NULL;
```

Results:

| Metric | Rows |
| --- | ---: |
| `qrt_rows` | `557,478` |
| `qrt_distinct_road_id` | `158,532` |
| `qrt_distinct_nonblank_road_id` | `158,532` |
| `lalf_roads_qt_false` | `5,717` |
| `lalf_roads_qt_true` | `112,933` |
| `lalf_roads_qt_null` | `163,077` |

The expected road RDF count is:

```text
158,532 distinct QRT road IDs + 5,717 LALF-only missing-QRT roads = 164,249
```

### Road Address References

QRT roads are converted from `qrt_spatial` regardless of whether current LALF addresses reference them. LALF-only missing-QRT roads are generated only when current addresses reference them.

```sql
WITH qrt AS (
  SELECT DISTINCT road_id
  FROM public.qrt_spatial
  WHERE road_id IS NOT NULL AND road_id <> ''
),
qrt_referenced AS (
  SELECT DISTINCT r.qrt_road_id AS road_id
  FROM public."lalfpdba.lf_address" a
  JOIN public."lalfpdba.lf_road" r ON r.road_id = a.road_id
  WHERE a.addr_status_code <> 'H'
    AND r.qrt_road_id IS NOT NULL
),
lalf_missing AS (
  SELECT DISTINCT r.road_id
  FROM public."lalfpdba.lf_road" r
  WHERE r.qrt_found IS FALSE
),
lalf_missing_referenced AS (
  SELECT DISTINCT r.road_id
  FROM public."lalfpdba.lf_address" a
  JOIN public."lalfpdba.lf_road" r ON r.road_id = a.road_id
  WHERE a.addr_status_code <> 'H'
    AND r.qrt_found IS FALSE
)
SELECT
  'qrt_roads' AS category,
  count(*)::bigint AS total_roads,
  count(ref.road_id)::bigint AS referenced_by_current_addresses,
  count(*) FILTER (WHERE ref.road_id IS NULL)::bigint AS not_referenced_by_current_addresses
FROM qrt q
LEFT JOIN qrt_referenced ref USING (road_id)
UNION ALL
SELECT
  'lalf_missing_qrt_roads',
  count(*)::bigint,
  count(ref.road_id)::bigint,
  count(*) FILTER (WHERE ref.road_id IS NULL)::bigint
FROM lalf_missing m
LEFT JOIN lalf_missing_referenced ref USING (road_id);
```

Results:

| Category | Total roads | Referenced by current addresses | Not referenced by current addresses |
| --- | ---: | ---: | ---: |
| `qrt_roads` | `158,532` | `99,812` | `58,720` |
| `lalf_missing_qrt_roads` | `5,717` | `5,717` | `0` |

Example QRT roads not referenced by current addresses:

```sql
WITH qrt AS (
  SELECT DISTINCT road_id, max(road_name_) AS road_name
  FROM public.qrt_spatial
  WHERE road_id IS NOT NULL AND road_id <> ''
  GROUP BY road_id
),
referenced AS (
  SELECT DISTINCT r.qrt_road_id AS road_id
  FROM public."lalfpdba.lf_address" a
  JOIN public."lalfpdba.lf_road" r ON r.road_id = a.road_id
  WHERE a.addr_status_code <> 'H'
    AND r.qrt_road_id IS NOT NULL
)
SELECT q.road_id, q.road_name
FROM qrt q
LEFT JOIN referenced r USING (road_id)
WHERE r.road_id IS NULL
ORDER BY q.road_id
LIMIT 10;
```

Results:

| Road ID | Road name |
| --- | --- |
| `7777015` | `245 Road` |
| `7777018` | `508 Road` |
| `7777019` | `684 Road` |
| `QLDR1031523491525482743220` | `Road` |
| `QLDR1061454312316237772810` | `1069 Road` |
| `QLDR1071488599224155752270` | `107th Street` |
| `QLDR1071488844624170352270` | `107th Street` |
| `QLDR1081453508315935052810` | `1080 Road` |
| `QLDR1081488948324172072270` | `108th Street` |
| `QLDR1091489093124176892270` | `109th Street` |

### Parcel Counts

```sql
SELECT 'parcels_total' AS metric, count(*)::bigint AS rows
FROM public."lalfpdba.lf_parcel"
UNION ALL
SELECT 'parcels_deleted', count(*)::bigint
FROM public."lalfpdba.lf_parcel"
WHERE parcel_status_code = 'D'
UNION ALL
SELECT 'parcels_not_deleted', count(*)::bigint
FROM public."lalfpdba.lf_parcel"
WHERE parcel_status_code IS DISTINCT FROM 'D';
```

Results:

| Metric | Rows |
| --- | ---: |
| `parcels_total` | `4,320,676` |
| `parcels_deleted` | `987,173` |
| `parcels_not_deleted` | `3,333,503` |

### Address To Site And Parcel Joins

```sql
SELECT 'current_address_site_join_rows' AS metric, count(*)::bigint AS rows
FROM public."lalfpdba.lf_address" a
JOIN public."lalfpdba.lf_site" s ON s.site_id = a.site_id
WHERE a.addr_status_code <> 'H'
UNION ALL
SELECT 'current_address_parcel_join_rows', count(*)::bigint
FROM public."lalfpdba.lf_address" a
JOIN public."lalfpdba.lf_site" s ON s.site_id = a.site_id
JOIN public."lalfpdba.lf_parcel" p ON p.parcel_id = s.parcel_id
WHERE a.addr_status_code <> 'H'
UNION ALL
SELECT 'distinct_parcel_ids_for_current_addresses', count(DISTINCT p.parcel_id)::bigint
FROM public."lalfpdba.lf_address" a
JOIN public."lalfpdba.lf_site" s ON s.site_id = a.site_id
JOIN public."lalfpdba.lf_parcel" p ON p.parcel_id = s.parcel_id
WHERE a.addr_status_code <> 'H';
```

Results:

| Metric | Rows |
| --- | ---: |
| `current_address_site_join_rows` | `2,833,300` |
| `current_address_parcel_join_rows` | `2,833,300` |
| `distinct_parcel_ids_for_current_addresses` | `2,627,958` |

## Fuseki Checks

These queries were run against `http://localhost:3030/ds/query`.

### Named Graph Triple Counts

```sparql
SELECT ?g (COUNT(*) AS ?triples)
WHERE {
  GRAPH ?g {
    ?s ?p ?o
  }
}
GROUP BY ?g
ORDER BY DESC(?triples)
```

Results:

| Graph | Triples |
| --- | ---: |
| `urn:qali:graph:addresses` | `135,238,944` |
| `urn:qali:graph:geographical-names` | `4,986,444` |
| `urn:qali:graph:roads` | `2,391,312` |
| `urn:qali:graph:vocabs` | `13,644` |
| `urn:qali:graph:users` | `89` |
| `urn:qali:graph:system` | `73` |
| `urn:qali:graph:tags` | `33` |

### Class Counts

```sparql
SELECT ?g ?class (COUNT(DISTINCT ?s) AS ?count)
WHERE {
  GRAPH ?g {
    ?s a ?class
  }
}
GROUP BY ?g ?class
ORDER BY ?g DESC(?count)
```

Relevant results:

| Graph | Class | Count |
| --- | --- | ---: |
| `urn:qali:graph:addresses` | `addr:AddressableObject` | `3,333,503` |
| `urn:qali:graph:addresses` | `addr:Address` | `2,833,300` |
| `urn:qali:graph:addresses` | `cn:CompoundName` | `2,833,300` |
| `urn:qali:graph:geographical-names` | `gn:GeographicalName` | `308,220` |
| `urn:qali:graph:geographical-names` | `cn:CompoundName` | `308,220` |
| `urn:qali:graph:geographical-names` | `gn:GeographicalObject` | `301,481` |
| `urn:qali:graph:roads` | `roads:RoadObject` | `164,249` |
| `urn:qali:graph:roads` | `roads:RoadName` | `164,249` |
| `urn:qali:graph:roads` | `cn:CompoundName` | `164,249` |
| `urn:qali:graph:vocabs` | `skos:Concept` | `1,311` |
| `urn:qali:graph:vocabs` | `skos:ConceptScheme` | `20` |

### Road Objects And Road Names

```sparql
PREFIX roads: <https://linked.data.gov.au/def/roads/>
PREFIX cn: <https://linked.data.gov.au/def/cn/>

SELECT (COUNT(DISTINCT ?road) AS ?roads) (COUNT(DISTINCT ?name) AS ?names)
WHERE {
  GRAPH <urn:qali:graph:roads> {
    ?road a roads:RoadObject ;
      cn:hasName ?name .
    ?name cn:isNameFor ?road .
  }
}
```

Results:

| Roads | Names |
| ---: | ---: |
| `164,249` | `164,249` |

### Road Identifier Datatypes

```sparql
PREFIX roads: <https://linked.data.gov.au/def/roads/>
PREFIX schema: <https://schema.org/>

SELECT ?datatype (COUNT(DISTINCT ?road) AS ?roads)
WHERE {
  GRAPH <urn:qali:graph:roads> {
    ?road a roads:RoadObject ;
      schema:identifier ?id .
    BIND(DATATYPE(?id) AS ?datatype)
  }
}
GROUP BY ?datatype
ORDER BY ?datatype
```

Results:

| Datatype | Roads |
| --- | ---: |
| `https://linked.data.gov.au/dataset/qld-addr/datatype/qrt-id` | `164,249` |

### Address Status Counts

```sparql
PREFIX addr: <https://linked.data.gov.au/def/addr/>

SELECT ?status (COUNT(DISTINCT ?a) AS ?addresses)
WHERE {
  GRAPH <urn:qali:graph:addresses> {
    ?a a addr:Address ;
      addr:hasStatus ?status .
  }
}
GROUP BY ?status
ORDER BY ?status
```

Results:

| Status | Addresses |
| --- | ---: |
| `https://linked.data.gov.au/def/addr-status-type/alternative` | `95,453` |
| `https://linked.data.gov.au/def/addr-status-type/primary` | `2,737,732` |
| `https://linked.data.gov.au/def/addr-status-type/unofficial` | `115` |

### Address To Addressable Object Links

```sparql
PREFIX addr: <https://linked.data.gov.au/def/addr/>
PREFIX cn: <https://linked.data.gov.au/def/cn/>

SELECT (COUNT(DISTINCT ?a) AS ?addresses) (COUNT(DISTINCT ?obj) AS ?objects)
WHERE {
  GRAPH <urn:qali:graph:addresses> {
    ?a a addr:Address ;
      cn:isNameFor ?obj .
    ?obj cn:hasName ?a .
  }
}
```

Results:

| Addresses | Objects |
| ---: | ---: |
| `2,833,300` | `2,627,958` |

### Addressable Object Types

```sparql
PREFIX addr: <https://linked.data.gov.au/def/addr/>
PREFIX schema: <https://schema.org/>

SELECT ?type (COUNT(DISTINCT ?obj) AS ?objects)
WHERE {
  GRAPH <urn:qali:graph:addresses> {
    ?obj a addr:AddressableObject ;
      schema:additionalType ?type .
  }
}
GROUP BY ?type
ORDER BY DESC(?objects)
```

Results:

| Type | Objects |
| --- | ---: |
| `https://linked.data.gov.au/def/go-categories/parcel` | `3,333,503` |

### Addressable Object Identifier Datatypes

```sparql
PREFIX addr: <https://linked.data.gov.au/def/addr/>
PREFIX schema: <https://schema.org/>

SELECT ?datatype (COUNT(*) AS ?ids) (COUNT(DISTINCT ?obj) AS ?objects)
WHERE {
  GRAPH <urn:qali:graph:addresses> {
    ?obj a addr:AddressableObject ;
      schema:identifier ?id .
    BIND(DATATYPE(?id) AS ?datatype)
  }
}
GROUP BY ?datatype
ORDER BY ?datatype
```

Results:

| Datatype | IDs | Objects |
| --- | ---: | ---: |
| `https://linked.data.gov.au/dataset/qld-addr/datatype/lot` | `3,333,503` | `3,333,503` |
| `https://linked.data.gov.au/dataset/qld-addr/datatype/lotplan` | `3,333,503` | `3,333,503` |
| `https://linked.data.gov.au/dataset/qld-addr/datatype/plan` | `3,333,503` | `3,333,503` |

### Geographical Object Identifier Datatypes

```sparql
PREFIX gn: <https://linked.data.gov.au/def/gn/>
PREFIX schema: <https://schema.org/>

SELECT ?datatype (COUNT(*) AS ?ids) (COUNT(DISTINCT ?obj) AS ?objects)
WHERE {
  GRAPH <urn:qali:graph:geographical-names> {
    ?obj a gn:GeographicalObject ;
      schema:identifier ?id .
    BIND(DATATYPE(?id) AS ?datatype)
  }
}
GROUP BY ?datatype
ORDER BY ?datatype
```

Results:

| Datatype | IDs | Objects |
| --- | ---: | ---: |
| `https://linked.data.gov.au/dataset/qld-addr/datatype/cisp` | `3` | `3` |
| `https://linked.data.gov.au/dataset/qld-addr/datatype/pndb` | `50,872` | `50,872` |
| `https://linked.data.gov.au/dataset/qld-addr/datatype/property` | `250,606` | `250,606` |

### Geographical Name Lifecycle Stage Additional Types

```sparql
PREFIX gn: <https://linked.data.gov.au/def/gn/>
PREFIX lifecycle: <https://linked.data.gov.au/def/lifecycle/>
PREFIX schema: <https://schema.org/>

SELECT
  ?additionalType
  (COUNT(*) AS ?links)
  (COUNT(DISTINCT ?name) AS ?names)
  (COUNT(DISTINCT ?stage) AS ?stages)
WHERE {
  GRAPH <urn:qali:graph:geographical-names> {
    ?name a gn:GeographicalName ;
      lifecycle:hasLifecycleStage ?stage .
    ?stage schema:additionalType ?additionalType .
  }
}
GROUP BY ?additionalType
ORDER BY DESC(?names) ?additionalType
```

Results:

| Additional type | Links | Names | Stages |
| --- | ---: | ---: | ---: |
| `https://linked.data.gov.au/def/lifecycle-stage-types/current` | `255,697` | `255,697` | `255,697` |
| `https://linked.data.gov.au/def/gn-statuses/gazetted` | `49,787` | `49,787` | `49,787` |
| `https://linked.data.gov.au/def/lifecycle-stage-types/retired` | `12,288` | `12,288` | `12,288` |
| `https://linked.data.gov.au/def/lifecycle-stage-types/unknown` | `254` | `254` | `254` |
| `https://linked.data.gov.au/def/gn-statuses/informal` | `3` | `3` | `3` |

Distribution of lifecycle stage `sdo:additionalType` values per geographical name:

```sparql
PREFIX gn: <https://linked.data.gov.au/def/gn/>
PREFIX lifecycle: <https://linked.data.gov.au/def/lifecycle/>
PREFIX schema: <https://schema.org/>

SELECT ?typeCount (COUNT(*) AS ?names)
WHERE {
  {
    SELECT ?name (COUNT(?additionalType) AS ?typeCount)
    WHERE {
      GRAPH <urn:qali:graph:geographical-names> {
        ?name a gn:GeographicalName ;
          lifecycle:hasLifecycleStage ?stage .
        ?stage schema:additionalType ?additionalType .
      }
    }
    GROUP BY ?name
  }
}
GROUP BY ?typeCount
ORDER BY ?typeCount
```

Results:

| Lifecycle stage `sdo:additionalType` count | Names |
| ---: | ---: |
| `1` | `298,411` |
| `2` | `9,809` |

### Geographical Object Additional Types

```sparql
PREFIX gn: <https://linked.data.gov.au/def/gn/>
PREFIX schema: <https://schema.org/>

SELECT ?additionalType (COUNT(*) AS ?links) (COUNT(DISTINCT ?object) AS ?objects)
WHERE {
  GRAPH <urn:qali:graph:geographical-names> {
    ?object a gn:GeographicalObject ;
      schema:additionalType ?additionalType .
  }
}
GROUP BY ?additionalType
ORDER BY DESC(?objects) ?additionalType
```

Results:

| Additional type | Links | Objects |
| --- | ---: | ---: |
| `https://linked.data.gov.au/def/go-categories/property` | `250,606` | `250,606` |
| `https://linked.data.gov.au/def/go-categories/watercourse` | `15,721` | `15,721` |
| `https://linked.data.gov.au/def/go-categories/locality` | `10,049` | `10,049` |
| `https://linked.data.gov.au/def/go-categories/parish` | `5,303` | `5,303` |
| `https://linked.data.gov.au/def/go-categories/waterhole` | `2,266` | `2,266` |
| `https://linked.data.gov.au/def/go-categories/mountain` | `2,245` | `2,245` |
| `https://linked.data.gov.au/def/go-categories/protected-area` | `1,597` | `1,597` |
| `https://linked.data.gov.au/def/go-categories/rail-station` | `1,470` | `1,470` |
| `https://linked.data.gov.au/def/go-categories/population-centre` | `1,131` | `1,131` |
| `https://linked.data.gov.au/def/go-categories/island` | `996` | `996` |
| `https://linked.data.gov.au/def/go-categories/coral-reef` | `764` | `764` |
| `https://linked.data.gov.au/def/go-categories/hill` | `736` | `736` |
| `https://linked.data.gov.au/def/go-categories/unclassified` | `700` | `700` |
| `https://linked.data.gov.au/def/go-categories/homestead` | `683` | `683` |
| `https://linked.data.gov.au/def/go-categories/point` | `642` | `642` |
| `https://linked.data.gov.au/def/go-categories/bore` | `547` | `547` |
| `https://linked.data.gov.au/def/go-categories/state-forest` | `539` | `539` |
| `https://linked.data.gov.au/def/go-categories/range` | `475` | `475` |
| `https://linked.data.gov.au/def/go-categories/rock` | `422` | `422` |
| `https://linked.data.gov.au/def/go-categories/peak` | `373` | `373` |
| `https://linked.data.gov.au/def/go-categories/lake` | `326` | `326` |
| `https://linked.data.gov.au/def/go-categories/county` | `322` | `322` |
| `https://linked.data.gov.au/def/go-categories/bay` | `290` | `290` |
| `https://linked.data.gov.au/def/go-categories/waterfall` | `263` | `263` |
| `https://linked.data.gov.au/def/go-categories/marine-shoal` | `245` | `245` |
| `https://linked.data.gov.au/def/go-categories/beach` | `205` | `205` |
| `https://linked.data.gov.au/def/go-categories/wetland` | `202` | `202` |
| `https://linked.data.gov.au/def/go-categories/sea-channel` | `134` | `134` |
| `https://linked.data.gov.au/def/go-categories/lookout` | `132` | `132` |
| `https://linked.data.gov.au/def/go-categories/pass` | `132` | `132` |
| `https://linked.data.gov.au/def/go-categories/ford` | `129` | `129` |
| `https://linked.data.gov.au/def/go-categories/marine-bank` | `122` | `122` |
| `https://linked.data.gov.au/def/go-categories/spring` | `119` | `119` |
| `https://linked.data.gov.au/def/go-categories/cape` | `109` | `109` |
| `https://linked.data.gov.au/def/go-categories/neighbourhood` | `97` | `97` |
| `https://linked.data.gov.au/def/go-categories/island-group` | `93` | `93` |
| `https://linked.data.gov.au/def/go-categories/aquatic-passage` | `91` | `91` |
| `https://linked.data.gov.au/def/go-categories/stockyard` | `90` | `90` |
| `https://linked.data.gov.au/def/go-categories/plain` | `85` | `85` |
| `https://linked.data.gov.au/def/go-categories/cliff` | `77` | `77` |
| `https://linked.data.gov.au/def/go-categories/reservoir` | `75` | `75` |
| `https://linked.data.gov.au/def/go-categories/reach` | `63` | `63` |
| `https://linked.data.gov.au/def/go-categories/cay` | `60` | `60` |
| `https://linked.data.gov.au/def/go-categories/inlet` | `56` | `56` |
| `https://linked.data.gov.au/def/go-categories/dune` | `46` | `46` |
| `https://linked.data.gov.au/def/go-categories/pocket` | `43` | `43` |
| `https://linked.data.gov.au/def/go-categories/ridge` | `41` | `41` |
| `https://linked.data.gov.au/def/go-categories/gorge` | `40` | `40` |
| `https://linked.data.gov.au/def/go-categories/harbour` | `38` | `38` |
| `https://linked.data.gov.au/def/go-categories/lagoon` | `37` | `37` |
| `https://linked.data.gov.au/def/go-categories/weir` | `33` | `33` |
| `https://linked.data.gov.au/def/go-categories/bar` | `30` | `30` |
| `https://linked.data.gov.au/def/go-categories/land-district` | `29` | `29` |
| `https://linked.data.gov.au/def/go-categories/cave` | `28` | `28` |
| `https://linked.data.gov.au/def/go-categories/dam-wall` | `25` | `25` |
| `https://linked.data.gov.au/def/go-categories/gate` | `23` | `23` |
| `https://linked.data.gov.au/def/go-categories/plateau` | `23` | `23` |
| `https://linked.data.gov.au/def/go-categories/spit` | `23` | `23` |
| `https://linked.data.gov.au/def/go-categories/corner` | `21` | `21` |
| `https://linked.data.gov.au/def/go-categories/entrance` | `20` | `20` |
| `https://linked.data.gov.au/def/go-categories/port` | `20` | `20` |
| `https://linked.data.gov.au/def/go-categories/water-tank` | `20` | `20` |
| `https://linked.data.gov.au/def/go-categories/peninsula` | `16` | `16` |
| `https://linked.data.gov.au/def/go-categories/anchorage` | `15` | `15` |
| `https://linked.data.gov.au/def/go-categories/pastoral-district` | `15` | `15` |
| `https://linked.data.gov.au/def/go-categories/landing-place` | `14` | `14` |
| `https://linked.data.gov.au/def/go-categories/school` | `13` | `13` |
| `https://linked.data.gov.au/def/go-categories/breakwater` | `9` | `9` |
| `https://linked.data.gov.au/def/go-categories/valley` | `8` | `8` |
| `https://linked.data.gov.au/def/go-categories/landing-area` | `6` | `6` |
| `https://linked.data.gov.au/def/go-categories/pan` | `6` | `6` |
| `https://linked.data.gov.au/def/go-categories/park` | `6` | `6` |
| `https://linked.data.gov.au/def/go-categories/historic-site` | `5` | `5` |
| `https://linked.data.gov.au/def/go-categories/junction` | `5` | `5` |
| `https://linked.data.gov.au/def/go-categories/sound` | `5` | `5` |
| `https://linked.data.gov.au/def/go-categories/tourist-region` | `4` | `4` |
| `https://linked.data.gov.au/def/go-categories/crater` | `3` | `3` |
| `https://linked.data.gov.au/def/go-categories/drain` | `3` | `3` |
| `https://linked.data.gov.au/def/go-categories/outstation` | `3` | `3` |
| `https://linked.data.gov.au/def/go-categories/rockhole` | `3` | `3` |
| `https://linked.data.gov.au/def/go-categories/isthmus` | `2` | `2` |
| `https://linked.data.gov.au/def/go-categories/marine-ridge` | `2` | `2` |
| `https://linked.data.gov.au/def/go-categories/marine-shelf` | `2` | `2` |
| `https://linked.data.gov.au/def/go-categories/rapids` | `2` | `2` |
| `https://linked.data.gov.au/def/go-categories/siding` | `2` | `2` |
| `https://linked.data.gov.au/def/go-categories/soak` | `2` | `2` |
| `https://linked.data.gov.au/def/go-categories/deep` | `1` | `1` |
| `https://linked.data.gov.au/def/go-categories/desert` | `1` | `1` |
| `https://linked.data.gov.au/def/go-categories/escarpment` | `1` | `1` |
| `https://linked.data.gov.au/def/go-categories/forest` | `1` | `1` |
| `https://linked.data.gov.au/def/go-categories/gulf` | `1` | `1` |
| `https://linked.data.gov.au/def/go-categories/marine-plateau` | `1` | `1` |
| `https://linked.data.gov.au/def/go-categories/scrub` | `1` | `1` |
| `https://linked.data.gov.au/def/go-categories/state` | `1` | `1` |

Distribution of `sdo:additionalType` values per geographical object:

```sparql
PREFIX gn: <https://linked.data.gov.au/def/gn/>
PREFIX schema: <https://schema.org/>

SELECT ?typeCount (COUNT(*) AS ?objects)
WHERE {
  {
    SELECT ?object (COUNT(?additionalType) AS ?typeCount)
    WHERE {
      GRAPH <urn:qali:graph:geographical-names> {
        ?object a gn:GeographicalObject ;
          schema:additionalType ?additionalType .
      }
    }
    GROUP BY ?object
  }
}
GROUP BY ?typeCount
ORDER BY ?typeCount
```

Results:

| `sdo:additionalType` count | Objects |
| ---: | ---: |
| `1` | `301,481` |

### Single `sdo:name` Values

```sparql
PREFIX schema: <https://schema.org/>
PREFIX roads: <https://linked.data.gov.au/def/roads/>
PREFIX addr: <https://linked.data.gov.au/def/addr/>
PREFIX gn: <https://linked.data.gov.au/def/gn/>

SELECT
  ?kind
  (COUNT(?s) AS ?total)
  (SUM(IF(?nameCount = 1, 1, 0)) AS ?exactlyOne)
  (SUM(IF(?nameCount = 0, 1, 0)) AS ?zeroNames)
  (SUM(IF(?nameCount > 1, 1, 0)) AS ?multipleNames)
  (MAX(?nameCount) AS ?maxNames)
WHERE {
  {
    SELECT ?kind ?s (COUNT(?name) AS ?nameCount)
    WHERE {
      VALUES (?kind ?graph ?class) {
        ("road names" <urn:qali:graph:roads> roads:RoadName)
        ("addresses" <urn:qali:graph:addresses> addr:Address)
        ("geographical names" <urn:qali:graph:geographical-names> gn:GeographicalName)
      }

      GRAPH ?graph {
        ?s a ?class .
        OPTIONAL {
          ?s schema:name ?name
        }
      }
    }
    GROUP BY ?kind ?s
  }
}
GROUP BY ?kind
ORDER BY ?kind
```

Results:

| Kind | Total | Exactly one | Zero names | Multiple names | Max names |
| --- | ---: | ---: | ---: | ---: | ---: |
| `addresses` | `2,833,300` | `2,833,300` | `0` | `0` | `1` |
| `geographical names` | `308,220` | `308,220` | `0` | `0` | `1` |
| `road names` | `164,249` | `164,249` | `0` | `0` | `1` |
