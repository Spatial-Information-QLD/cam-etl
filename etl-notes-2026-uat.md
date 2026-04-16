Address count matches. 2,830,543

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
