# ETL Queries

These queries compare the data in the LALF database to the data produced by the ETL.

## Property Names

Total Property Names:

```sql
select count(distinct id)
from lalf_property_address_joined
```

Result: 66300

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
