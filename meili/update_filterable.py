import meilisearch

client = meilisearch.Client(
    "http://127.0.0.1:7700",
)

index = client.index("addresses")
index.update_filterable_attributes(["status"])
