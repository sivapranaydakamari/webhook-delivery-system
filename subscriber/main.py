"""
Subscriber Simulator Service.
Mimics a real-world webhook consumer with configurable latency and error rates.
Validates incoming webhooks and enforces idempotency.
"""

import asyncio
import hashlib
import hmac
import json
import logging
import os
import random
import time
from contextlib import asynccontextmanager

import httpx
import redis
from fastapi import FastAPI, Request, Response

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


REDIS_HOST            = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT            = int(os.getenv("REDIS_PORT", 6379))
ADMIN_API_URL         = os.getenv("ADMIN_API_URL", "http://admin:8002")
SELF_URL              = os.getenv("SELF_URL", "http://subscriber:8000/webhook")
TIMESTAMP_TOLERANCE   = int(os.getenv("TIMESTAMP_TOLERANCE_SECONDS", 300))
RESPONSE_LATENCY_MS   = int(os.getenv("RESPONSE_LATENCY_MS", 50))
ERROR_RATE            = float(os.getenv("ERROR_RATE", 0.0))


redis_client = redis.Redis(
    host=REDIS_HOST, port=REDIS_PORT, decode_responses=True
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Register with the Admin API at startup."""
    global subscriber_id, subscriber_secret


    stored_id     = redis_client.get(SELF_ID_KEY)
    stored_secret = redis_client.get(SELF_SECRET_KEY)

    if stored_id and stored_secret:
        subscriber_id     = stored_id
        subscriber_secret = stored_secret
        logger.info(f"Restored registration: subscriber_id={subscriber_id}")
    else:
        
        registered = False
        for i in range(30):
            try:
                async with httpx.AsyncClient() as client:
                    resp = await client.get(f"{ADMIN_API_URL}/health", timeout=5.0)
                    if resp.status_code == 200:
                        
                        reg = await client.post(
                            f"{ADMIN_API_URL}/subscribers",
                            json={"url": SELF_URL},
                            timeout=10.0,
                        )
                        if reg.status_code == 201:
                            data = reg.json()
                            subscriber_id     = data["subscriber_id"]
                            subscriber_secret = data["secret_key"]
                            
                            redis_client.set(SELF_ID_KEY,     subscriber_id)
                            redis_client.set(SELF_SECRET_KEY, subscriber_secret)
                            logger.info(
                                f"Registered with Admin API: "
                                f"subscriber_id={subscriber_id}"
                            )
                            registered = True
                            break
            except Exception as exc:
                logger.info(f"Waiting for Admin API... ({i+1}/30): {exc}")
            await asyncio.sleep(2)

        if not registered:
            logger.error("Could not register with Admin API — proceeding anyway.")

    yield


app = FastAPI(title="Subscriber Simulator", version="1.0", lifespan=lifespan)


def _verify_hmac(body: bytes, signature_header: str, secret: str) -> bool:
    """Recompute the HMAC-SHA256 digest to verify the signature."""
    expected_digest = hmac.new(
        secret.encode("utf-8"),
        msg=body,
        digestmod=hashlib.sha256,
    ).hexdigest()
    expected_header = f"sha256={expected_digest}"
    return hmac.compare_digest(signature_header, expected_header)


    """Process incoming webhooks after applying security validations."""
    if RESPONSE_LATENCY_MS > 0:
        await asyncio.sleep(RESPONSE_LATENCY_MS / 1000.0)

    if ERROR_RATE > 0 and random.random() < ERROR_RATE:
        return Response(status_code=503, content="Simulated server error")


    required = [
        "x-webhook-signature",
        "x-webhook-timestamp",
        "x-request-id",
        "x-webhook-version",
        "x-delivery-attempt",
    ]
    missing = [h for h in required if h not in request.headers]
    if missing:
        logger.warning(f"Missing headers: {missing}")
        return Response(
            status_code=400,
            content=f"Missing required headers: {missing}",
        )

    
    signature      = request.headers["x-webhook-signature"]
    timestamp_str  = request.headers["x-webhook-timestamp"]
    request_id     = request.headers["x-request-id"]
    version        = request.headers["x-webhook-version"]
    attempt        = request.headers["x-delivery-attempt"]

    
    if version != "v1":
        logger.warning(f"Unsupported webhook version: {version}")
        return Response(status_code=400, content=f"Unsupported version: {version}")

    
    try:
        timestamp = int(timestamp_str)
    except ValueError:
        return Response(status_code=400, content="Invalid timestamp format")

    age_seconds = int(time.time()) - timestamp
    if abs(age_seconds) > TIMESTAMP_TOLERANCE:
        logger.warning(
            f"Stale timestamp: age={age_seconds}s tolerance={TIMESTAMP_TOLERANCE}s"
        )
        return Response(
            status_code=400,
            content=(
                f"Timestamp too old ({age_seconds}s). "
                f"Max allowed: {TIMESTAMP_TOLERANCE}s"
            ),
        )

    
    if subscriber_secret is None:
        logger.error("Subscriber secret not configured!")
        return Response(status_code=500, content="Subscriber not configured")

    body = await request.body()

    if not _verify_hmac(body, signature, subscriber_secret):
        logger.warning(f"Signature mismatch for request_id={request_id}")
        return Response(status_code=401, content="Signature verification failed")

    
    idempotency_key = f"idempotency:{request_id}"
    is_new = redis_client.set(idempotency_key, 1, ex=86400, nx=True)

    if not is_new:
        logger.info(f"Duplicate request_id={request_id} — ignoring")
        return {"status": "already_processed", "request_id": request_id}

    
    try:
        event = json.loads(body)
        event_type = event.get("event_type", "unknown")
        logger.info(
            f"Processing event: type={event_type} "
            f"request_id={request_id} attempt={attempt}"
        )

        
        log_entry = json.dumps({
            "request_id": request_id,
            "event_type": event_type,
            "event":      event,
            "attempt":    attempt,
            "processed_at": int(time.time()),
        })
        with open("/tmp/processed_events.log", "a") as fh:
            fh.write(log_entry + "\n")

    except Exception as exc:
        logger.error(f"Error processing event: {exc}")
        

    return {"status": "processed", "request_id": request_id}


    """Health check endpoint."""
    try:
        redis_client.ping()
        return {
            "status": "healthy",
            "subscriber_id": subscriber_id,
            "self_url": SELF_URL,
            "redis": "ok",
        }
    except Exception:
        return {"status": "degraded", "redis": "unreachable"}


@app.get("/processed")
def get_processed_events():
    """Return the list of processed events."""
    try:
        with open("/tmp/processed_events.log") as fh:
            events = [json.loads(line) for line in fh if line.strip()]
        return {"count": len(events), "events": events}
    except FileNotFoundError:
        return {"count": 0, "events": []}
