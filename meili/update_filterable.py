import meilisearch

client = meilisearch.Client(
    "http://localhost:7700",
)

index = client.index("addresses")
index.update_filterable_attributes(["status"])
