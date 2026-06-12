"""
Admin API Service.
Manages webhook subscriber registrations and their credentials.
"""

import logging
import os
import uuid

import redis
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(title="Admin API", version="1.0")

redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    decode_responses=True,
)

SUBSCRIBERS_SET = "subscribers"
SUBSCRIBER_KEY  = "subscriber:{}"


class SubscriberRequest(BaseModel):
    url: str


@app.post("/subscribers", status_code=201)
def register_subscriber(req: SubscriberRequest):
    """Register a new webhook subscriber, returning its ID and secret key."""
    subscriber_id = str(uuid.uuid4())
    secret_key = uuid.uuid4().hex + uuid.uuid4().hex[:16]

    redis_client.hset(
        SUBSCRIBER_KEY.format(subscriber_id),
        mapping={
            "subscriber_id": subscriber_id,
            "url": req.url,
            "secret_key": secret_key,
        },
    )

    redis_client.sadd(SUBSCRIBERS_SET, subscriber_id)

    logger.info(f"Registered subscriber {subscriber_id} → {req.url}")

    return {
        "subscriber_id": subscriber_id,
        "url": req.url,
        "secret_key": secret_key,
    }


@app.get("/subscribers")
def list_subscribers():
    """Return all registered subscribers."""
    ids = redis_client.smembers(SUBSCRIBERS_SET)
    result = []
    for sub_id in ids:
        data = redis_client.hgetall(SUBSCRIBER_KEY.format(sub_id))
        if data:
            result.append(data)
    return {"subscribers": result, "count": len(result)}


@app.get("/subscribers/{subscriber_id}")
def get_subscriber(subscriber_id: str):
    """Fetch a single subscriber by ID."""
    data = redis_client.hgetall(SUBSCRIBER_KEY.format(subscriber_id))
    if not data:
        raise HTTPException(status_code=404, detail="Subscriber not found")
    return data


@app.delete("/subscribers/{subscriber_id}", status_code=200)
def delete_subscriber(subscriber_id: str):
    """Remove a subscriber's registration data."""
    key = SUBSCRIBER_KEY.format(subscriber_id)
    if not redis_client.exists(key):
        raise HTTPException(status_code=404, detail="Subscriber not found")

    redis_client.delete(key)
    redis_client.srem(SUBSCRIBERS_SET, subscriber_id)

    logger.info(f"Deleted subscriber {subscriber_id}")
    return {"status": "deleted", "subscriber_id": subscriber_id}


@app.get("/health")
def health():
    """Health-check endpoint."""
    try:
        redis_client.ping()
        return {"status": "healthy", "redis": "ok"}
    except Exception:
        raise HTTPException(status_code=503, detail="Redis unreachable")
