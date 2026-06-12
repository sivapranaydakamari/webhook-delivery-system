"""
Event Producer Service.
Accepts business events and appends them to a durable Redis Stream queue.
"""

import json
import logging
import os

import redis
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Any, Dict

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(title="Event Producer", version="1.0")

    decode_responses=True,
)

STREAM_NAME = "webhook_events"


class EventRequest(BaseModel):
    event_type: str
    payload: Dict[str, Any]
    priority: str = "normal"


@app.post("/events", status_code=201)
def create_event(event: EventRequest):
    """Accept a new event and publish it to the Redis Stream."""
    try:
        event_data = json.dumps({
            "event_type": event.event_type,
            "payload": event.payload,
            "priority": event.priority,
        })

        message_id = redis_client.xadd(STREAM_NAME, {"data": event_data})

        logger.info(f"Published event {message_id}: type={event.event_type} priority={event.priority}")

        return {
            "status": "queued",
            "message_id": message_id,
            "event_type": event.event_type,
            "priority": event.priority,
        }

    except redis.RedisError as exc:
        logger.error(f"Redis error: {exc}")
        raise HTTPException(status_code=500, detail="Failed to queue event")


@app.get("/health")
def health():
    """Health-check endpoint."""
    try:
        redis_client.ping()
        return {"status": "healthy", "redis": "ok"}
    except Exception:
        raise HTTPException(status_code=503, detail="Redis unreachable")
