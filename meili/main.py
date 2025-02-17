import redis
import uvicorn
from rdflib import Namespace
from fastapi import FastAPI, Request
from redis.commands.search.query import Query
from typing import Annotated
from fastapi import Query as QueryParam

ADDR = Namespace("https://linked.data.gov.au/def/addr/")

# Configuration constants
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_INDEX = "idx:address"

# Initialize Redis with error handling
try:
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    # Test connection
    redis_client.ping()
except redis.ConnectionError as e:
    print(f"Failed to connect to Redis: {e}")
    raise

app = FastAPI()


@app.get("/")
def home_route():
    return {"message": "Hello World"}


@app.get("/status-values")
def status_values_route(request: Request):
    # Get unique status values using FT.TAGVALS
    status_values = redis_client.ft("idx:address").tagvals("status")
    base_url = str(request.base_url).rstrip("/")

    return {
        "data": [
            {
                "value": status,
                "links": {"addresses": f"{base_url}/addresses?filter[status]={status}"},
            }
            for status in status_values
        ]
    }


def build_search_query(
    q: str = "",
    identifier: str = "",
    lot: str = "",
    plan: str = "",
    status: str | None = None,
) -> str:
    """Build Redis search query string from parameters."""
    query_parts = []

    if q:
        q = " ".join(q.replace("-", " ").replace("/", " ").split())
        query_parts.append(f"@label:{q}*")

    if identifier:
        identifier = identifier.replace("-", " ").replace("/", " ")
        query_parts.append(f"@identifier:{identifier}*")

    if lot:
        lot = lot.replace("-", " ").replace("/", " ")
        query_parts.append(f"@lot:{lot}")

    if plan:
        plan = plan.replace("-", " ").replace("/", " ")
        query_parts.append(f"@plan:{plan}*")

    if status:
        escaped_status = (
            status.replace(":", "\\:")
            .replace("/", "\\/")
            .replace(".", "\\.")
            .replace("-", "\\-")
        )
        query_parts.append(f"@status:{{{escaped_status}}}")

    return " ".join(query_parts) if query_parts else "*"


# TODO: error handling according to JSON:API spec
@app.get("/addresses")
async def addresses_route(
    request: Request,
    q: Annotated[str, QueryParam()] = "",
    identifier: Annotated[str, QueryParam()] = "",
    lot: Annotated[str, QueryParam()] = "",
    plan: Annotated[str, QueryParam()] = "",
    page_offset: Annotated[int, QueryParam(alias="page[offset]", ge=0)] = 0,
    page_limit: Annotated[int, QueryParam(alias="page[limit]", ge=1, le=100)] = 10,
    status: Annotated[str | None, QueryParam(alias="filter[status]")] = None,
):
    try:
        # Build query string using the extracted function
        query_string = build_search_query(q, identifier, lot, plan, status)
        query = Query(query_string).sort_by("label").paging(page_offset, page_limit)

        result = redis_client.ft(REDIS_INDEX).search(query)

        # Update links to include lot and plan filters if present
        lot_param = f"&lot={lot}" if lot else ""
        plan_param = f"&plan={plan}" if plan else ""
        status_param = f"&filter[status]={status}" if status else ""
        base_url = str(request.base_url).rstrip("/")

        # Calculate prev and next offsets
        prev_offset = max(page_offset - page_limit, 0)
        next_offset = page_offset + page_limit

        # Only include next link if there are more results
        links = {
            "self": f"{base_url}/addresses?q={q}{status_param}{lot_param}{plan_param}&page[offset]={page_offset}&page[limit]={page_limit}",
        }

        # Only include first/last/prev/next links if there are multiple pages
        if result.total > page_limit:
            links["first"] = (
                f"{base_url}/addresses?q={q}{status_param}{lot_param}{plan_param}&page[offset]=0&page[limit]={page_limit}"
            )
            links["last"] = (
                f"{base_url}/addresses?q={q}{status_param}{lot_param}{plan_param}&page[offset]={result.total - page_limit}&page[limit]={page_limit}"
            )
        # Add prev link if we're not on the first page
        if page_offset > 0:
            links["prev"] = (
                f"{base_url}/addresses?q={q}{status_param}{lot_param}{plan_param}&page[offset]={prev_offset}&page[limit]={page_limit}"
            )
        # Add next link if there are more results
        if next_offset < result.total:
            links["next"] = (
                f"{base_url}/addresses?q={q}{status_param}{lot_param}{plan_param}&page[offset]={next_offset}&page[limit]={page_limit}"
            )
        return {
            "meta": {
                "total": result.total,
                "page": {
                    "offset": page_offset,
                    "limit": page_limit,
                },
            },
            "links": links,
            "data": [
                {
                    "type": ADDR.Address,
                    "id": row.iri,
                    "attributes": {
                        "identifier": row.identifier,
                        "label": row.label,
                        "status": row.status,
                        "parcel": row.parcel,
                        "lot": row.lot,
                        "plan": row.plan,
                    },
                }
                for row in result.docs
            ],
        }

    except redis.RedisError as e:
        return {"error": f"Database error: {str(e)}"}, 500


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
