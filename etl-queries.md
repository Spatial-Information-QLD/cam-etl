# ETL Queries

These queries compare the data in the LALF database to the data produced by the ETL.

- qrt
  - segment count
  - road object count
  - road label count
  - road names by segment id
  - road names by road id
- address
  - address road id the same as qrt road id
  - addressable objects count matches the count of parcels in lalf

## Property Names

### Property Names Count that do not join with LALF Parcels

LALF SQL:

```sql
select count(*)
from lalf_property_address_joined pa
left join "lalfpdba.lf_parcel" p on pa.lot = p.lot_no and pa.plan = p.plan_no
where p.lot_no is null and p.plan_no is null;
```

Result: 2208

### Property Names Count Matched

LALF SQL:

```sql
select count(*)
from (
	select distinct a.addr_id
	from lalf_property_address_joined pa
	join "lalfpdba.lf_parcel" p on p.lot_no = pa.lot and p.plan_no = pa.plan
	join "lalfpdba.lf_site" s on s.parcel_id = p.parcel_id
	join "lalfpdba.lf_address" a on a.site_id = s.site_id
) as sub
```

SPARQL:

```sparql
PREFIX addr: <https://linked.data.gov.au/def/addr/>
PREFIX apt: <https://linked.data.gov.au/def/addr-part-types/>
PREFIX cn: <https://linked.data.gov.au/def/cn/>
PREFIX sdo: <https://schema.org/>

SELECT (count(distinct ?addr) as ?count)
WHERE {
    GRAPH <urn:qali:graph:addresses> {
        ?addr sdo:hasPart [
        	sdo:additionalType apt:propertyName ;
            sdo:value ?propertyName
        ]
    }
}
```

What is the relationship between an address or a land parcel with a property name? Looks like the property names database contains multiple property names for the same address/parcel.

```sql
select pa.id, a.addr_id, count(*)
from lalf_property_address_joined pa
join "lalfpdba.lf_parcel" p on p.lot_no = pa.lot and p.plan_no = pa.plan
join "lalfpdba.lf_site" s on s.parcel_id = p.parcel_id
join "lalfpdba.lf_address" a on a.site_id = s.site_id
group by pa.id, a.addr_id
having count(*) > 1
```

### Total Property Names

```sql
select count(distinct id)
from lalf_property_address_joined
```

Result: 66300

todo: remove additional type and check that a GN -> GO -> GOC is "property"
todo: check via addr part type = property

```sparql
PREFIX gn: <https://linked.data.gov.au/def/gn/>
PREFIX sdo: <https://schema.org/>

select (count(?s) as ?count)
where {
    graph <urn:qali:graph:geographical-names> {
        ?s a gn:GeographicalName ;
        	sdo:additionalType <https://linked.data.gov.au/def/qld-gnt/PropertyName>
    }
}
```

Result: 66300

### Property Names linked to land parcel in addresses graph

LALF SQL:

```sql
select *
from lalf_property_address_joined p
where p.id = '36146'
```

Result:

| property_name  | lot | plan    | id    |
| -------------- | --- | ------- | ----- |
| Eastview Lodge | 0   | BUP6660 | 36146 |
| Eastview Lodge | 1   | BUP6660 | 36146 |
| Eastview Lodge | 2   | BUP6660 | 36146 |

SPARQL:

```sparql
PREFIX cn: <https://linked.data.gov.au/def/cn/>
PREFIX gn: <https://linked.data.gov.au/def/gn/>
PREFIX sdo: <https://schema.org/>

select ?iri ?addressableObject ?lotplan ?propertyName
where {
    graph <urn:qali:graph:geographical-names> {
        ?iri a gn:GeographicalName ;
        	sdo:additionalType <https://linked.data.gov.au/def/qld-gnt/PropertyName> ;
        	sdo:identifier "36146"^^<https://linked.data.gov.au/dataset/qld-addr/datatype/property> ;
        	cn:isNameFor ?addressableObject ;
        	sdo:name ?propertyName .
    }

    graph <urn:qali:graph:addresses> {
        ?addressableObject sdo:identifier ?lotplan
        filter(datatype(?lotplan) = <https://linked.data.gov.au/dataset/qld-addr/datatype/lotplan>)
    }
}
limit 10
```

|     | iri                                                                                 | addressableObject                                              | lotplan                                                                       | propertyName     |
| --- | ----------------------------------------------------------------------------------- | -------------------------------------------------------------- | ----------------------------------------------------------------------------- | ---------------- |
| 1   | https://linked.data.gov.au/dataset/qld-addr/gn/f25b1081-bae1-5b9e-8df6-51d2fbbe1872 | https://linked.data.gov.au/dataset/qld-addr/parcel/9999BUP6660 | "9999BUP6660"^^<https://linked.data.gov.au/dataset/qld-addr/datatype/lotplan> | "Eastview Lodge" |
| 2   | https://linked.data.gov.au/dataset/qld-addr/gn/f25b1081-bae1-5b9e-8df6-51d2fbbe1872 | https://linked.data.gov.au/dataset/qld-addr/parcel/2BUP6660    | "2BUP6660"^^<https://linked.data.gov.au/dataset/qld-addr/datatype/lotplan>    | "Eastview Lodge" |
| 3   | https://linked.data.gov.au/dataset/qld-addr/gn/f25b1081-bae1-5b9e-8df6-51d2fbbe1872 | https://linked.data.gov.au/dataset/qld-addr/parcel/1BUP6660    | "1BUP6660"^^<https://linked.data.gov.au/dataset/qld-addr/datatype/lotplan>    | "Eastview Lodge" |

## Geographic Names

### Total Geographic Names

LALF SQL:

```sql
select count(distinct reference_number)
from "pndb.place_name" pn
```

Result: 50646

SPARQL:

```sparql
PREFIX cn: <https://linked.data.gov.au/def/cn/>
PREFIX gn: <https://linked.data.gov.au/def/gn/>
PREFIX sdo: <https://schema.org/>

select (count(distinct ?identifier) as ?count)
where {
    graph <urn:qali:graph:geographical-names> {
        ?iri a gn:GeographicalName ;
        	cn:isNameFor ?obj .

        ?obj sdo:identifier ?identifier .
    }
}

```

Result: 50646

### Total Indigenous Names

LALF SQL:

```sql
select count(distinct reference_number)
from "pndb.indigenous_name" ipn
```

Result: 227

SPARQL:

```sparql
PREFIX cn: <https://linked.data.gov.au/def/cn/>
PREFIX gn: <https://linked.data.gov.au/def/gn/>
PREFIX sdo: <https://schema.org/>

select (count(distinct ?identifier) as ?count)
where {
    graph <urn:qali:graph:geographical-names> {
        ?iri a gn:GeographicalName ;
        	cn:hasAuthority <https://linked.data.gov.au/def/naming-authority/indigenous-group> ;
        	cn:isNameFor ?obj .

        ?obj sdo:identifier ?identifier .
    }
}

```

Result: 227

### Total Gazetted Names

LALF SQL:

```sql
select count(distinct reference_number)
from "pndb.place_name" pn
where pn.status = 'Y' and pn.currency = 'Y'
```

Result: 37250

SPARQL:

```sparql
PREFIX cn: <https://linked.data.gov.au/def/cn/>
PREFIX lc: <https://linked.data.gov.au/def/lifecycle/>
PREFIX gn: <https://linked.data.gov.au/def/gn/>
PREFIX sdo: <https://schema.org/>

select (count(distinct ?identifier) as ?count)
where {
    graph <urn:qali:graph:geographical-names> {
        ?iri a gn:GeographicalName ;
        lc:hasLifecycleStage [
                sdo:additionalType <https://linked.data.gov.au/def/gn-statuses/gazetted>
        ] ;
        cn:isNameFor ?obj .

        ?obj sdo:identifier ?identifier .

    }
}
```

Result: 37250

### Total Under the Place Names Authority

LALF SQL:

```sql
select count(distinct reference_number)
from "pndb.place_name" pn
where pn.status = 'Y' and pn.currency = 'Y' or pn.status = 'Y' and pn.currency = 'N' or pn.status = 'N' and pn.currency = 'N'
```

Result: 45926

SPARQL:

```sparql
PREFIX cn: <https://linked.data.gov.au/def/cn/>
PREFIX lc: <https://linked.data.gov.au/def/lifecycle/>
PREFIX gn: <https://linked.data.gov.au/def/gn/>
PREFIX sdo: <https://schema.org/>

select (count(distinct ?identifier) as ?count)
where {
    graph <urn:qali:graph:geographical-names> {
        ?iri a gn:GeographicalName ;
        	cn:hasAuthority <https://linked.data.gov.au/def/naming-authority/qld-pn-act-1994> ;
        cn:isNameFor ?obj .

        ?obj sdo:identifier ?identifier .

    }
}
```

Result: 45926

## Roads

### Segment Count

LALF SQL:

```sql
select count(segment_id)
from qrt_spatial q
```

Result: 543243

SPARQL:

```sparql
PREFIX cn: <https://linked.data.gov.au/def/cn/>
PREFIX lc: <https://linked.data.gov.au/def/lifecycle/>
PREFIX roads: <https://linked.data.gov.au/def/roads/>
PREFIX sdo: <https://schema.org/>

select (count(distinct ?iri) as ?count)
where {
    graph <urn:qali:graph:roads> {
        ?iri a roads:RoadSegment .
    }
}
```

Result: 543243

### Road Object Count

LALF SQL:

```sql
select count(distinct road_id)
from qrt_spatial q
```

Result: 155345

SPARQL:

```sparql
PREFIX cn: <https://linked.data.gov.au/def/cn/>
PREFIX lc: <https://linked.data.gov.au/def/lifecycle/>
PREFIX roads: <https://linked.data.gov.au/def/roads/>
PREFIX sdo: <https://schema.org/>

select (count(distinct ?iri) as ?count)
where {
    graph <urn:qali:graph:roads> {
        ?iri a roads:RoadObject .
    }
}
```

Result: 155345

### Road Name Count

The road name count in the RDF data should be the same as the road object count from above.

SPARQL:

```sparql
PREFIX cn: <https://linked.data.gov.au/def/cn/>
PREFIX lc: <https://linked.data.gov.au/def/lifecycle/>
PREFIX roads: <https://linked.data.gov.au/def/roads/>
PREFIX sdo: <https://schema.org/>

select (count(distinct ?iri) as ?count)
where {
    graph <urn:qali:graph:roads> {
        ?iri a roads:RoadName .
    }
}

```
