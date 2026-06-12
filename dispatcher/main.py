"""
Dispatcher Worker.
Reads events from Redis, queues them per subscriber, and delivers them via HTTP POST
while applying backpressure strategies.
"""

import hashlib
import hmac
import json
import logging
import os
import queue
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Dict

import httpx
import redis


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)s] %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


REDIS_HOST           = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT           = int(os.getenv("REDIS_PORT", 6379))
STREAM_NAME          = "webhook_events"
CONSUMER_GROUP       = "webhook_dispatcher"

CONSUMER_NAME        = f"dispatcher_{uuid.uuid4().hex[:8]}"

BACKPRESSURE_STRATEGY   = os.getenv("BACKPRESSURE_STRATEGY", "rate_limiting")
TOKEN_BUCKET_CAPACITY   = float(os.getenv("TOKEN_BUCKET_CAPACITY", 10))
TOKEN_REFILL_RATE       = float(os.getenv("TOKEN_REFILL_RATE", 2))   # tokens / second
HIGH_WATER_MARK         = int(os.getenv("HIGH_WATER_MARK", 100))     # admission control
LOW_WATER_MARK          = int(os.getenv("LOW_WATER_MARK", 50))       # admission control
SHEDDING_THRESHOLD      = int(os.getenv("SHEDDING_FAILURE_THRESHOLD", 3))
MAX_RETRIES             = int(os.getenv("MAX_RETRIES", 3))

QUEUE_DEPTH_LOG = "/tmp/logs/queue_depths.log"


redis_client = redis.Redis(
    host=REDIS_HOST, port=REDIS_PORT, decode_responses=True
)



@dataclass
class SubscriberState:
    subscriber_id: str
    url: str
    secret_key: str

    
    delivery_queue: queue.Queue = field(
        default_factory=lambda: queue.Queue(maxsize=200)
    )

    tokens: float      = field(default=None)
    last_refill: float = field(default_factory=time.time)
    token_lock: threading.Lock = field(default_factory=threading.Lock)


    consecutive_failures: int  = 0
    in_shedding_mode: bool     = False

    def __post_init__(self):
        
        if self.tokens is None:
            self.tokens = TOKEN_BUCKET_CAPACITY



pending_acks: Dict[str, int] = {}   # msg_id → remaining subscriber count
pending_lock = threading.Lock()


def mark_delivery_done(msg_id: str) -> None:
    """Acknowledge message in Redis once all subscribers finish processing it."""
    with pending_lock:
        if msg_id not in pending_acks:
            return
        pending_acks[msg_id] -= 1
        if pending_acks[msg_id] <= 0:
            del pending_acks[msg_id]
            try:
                redis_client.xack(STREAM_NAME, CONSUMER_GROUP, msg_id)
                logger.debug(f"XACK {msg_id}")
            except Exception as exc:
                logger.error(f"XACK failed for {msg_id}: {exc}")



def sign_payload(payload_bytes: bytes, secret_key: str) -> str:
    """Compute HMAC-SHA256 of the raw request body bytes."""
    digest = hmac.new(
        secret_key.encode("utf-8"),
        msg=payload_bytes,
        digestmod=hashlib.sha256,
    ).hexdigest()
    return f"sha256={digest}"



def try_consume_token(state: SubscriberState) -> bool:
    """Refill the token bucket and try to consume a token."""
    with state.token_lock:
        now = time.time()
        elapsed = now - state.last_refill
        
        state.tokens = min(
            TOKEN_BUCKET_CAPACITY,
            state.tokens + elapsed * TOKEN_REFILL_RATE,
        )
        state.last_refill = now

        if state.tokens >= 1.0:
            state.tokens -= 1.0
            return True
        return False


# ─────────────────────────────────────────────────────────────────────────────
# HTTP delivery attempt
# ─────────────────────────────────────────────────────────────────────────────
def deliver_once(
    state: SubscriberState,
    payload_bytes: bytes,
    request_id: str,
    attempt: int,
) -> int:
    """Send a single HTTP POST attempt to the subscriber."""
    timestamp = int(time.time())
    signature = sign_payload(payload_bytes, state.secret_key)

    headers = {
        "Content-Type": "application/json",
        "X-Webhook-Signature":  signature,
        "X-Webhook-Timestamp":  str(timestamp),
        "X-Request-ID":         request_id,
        "X-Webhook-Version":    "v1",
        "X-Delivery-Attempt":   str(attempt),
    }

    try:
        resp = httpx.post(
            state.url,
            content=payload_bytes,
            headers=headers,
            timeout=10.0,
        )
        return resp.status_code
    except Exception as exc:
        logger.warning(f"[{state.subscriber_id}] Network error attempt {attempt}: {exc}")
        return 0



def subscriber_worker(state: SubscriberState, strategy: str) -> None:
    """Run subscriber delivery tasks and apply backpressure strategy."""
    logger.info(
        f"Worker started for subscriber {state.subscriber_id} "
        f"(url={state.url}, strategy={strategy})"
    )

    while True:
        
        try:
            task = state.delivery_queue.get(timeout=1.0)
        except queue.Empty:
            continue

        
        if task is None:
            break

        msg_id, event, request_id = task
        payload_bytes = json.dumps(event).encode("utf-8")

        
        if strategy == "rate_limiting":
            wait_logged = False
            while not try_consume_token(state):
                if not wait_logged:
                    logger.debug(
                        f"[{state.subscriber_id}] Token bucket empty, waiting..."
                    )
                    wait_logged = True
                time.sleep(0.05)

        
        delivered = False
        for attempt in range(1, MAX_RETRIES + 1):
            status = deliver_once(state, payload_bytes, request_id, attempt)

            if status == 200:
                
                delivered = True
                state.consecutive_failures = 0
                state.in_shedding_mode = False
                logger.info(
                    f"[{state.subscriber_id}] Delivered msg={msg_id} "
                    f"attempt={attempt}"
                )
                break

            elif status in (429, 503):
                
                backoff = 2 ** attempt
                logger.warning(
                    f"[{state.subscriber_id}] Got {status}, backing off {backoff}s"
                )
                state.consecutive_failures += 1
                time.sleep(backoff)

            else:
                
                state.consecutive_failures += 1
                if attempt < MAX_RETRIES:
                    backoff = 2 ** (attempt - 1)
                    logger.warning(
                        f"[{state.subscriber_id}] Got {status}, retry in {backoff}s"
                    )
                    time.sleep(backoff)

        if not delivered:
            logger.error(
                f"[{state.subscriber_id}] Gave up on msg={msg_id} "
                f"after {MAX_RETRIES} attempts"
            )

        
        if strategy == "load_shedding":
            if state.consecutive_failures >= SHEDDING_THRESHOLD:
                if not state.in_shedding_mode:
                    logger.warning(
                        f"[{state.subscriber_id}] Entering SHEDDING MODE "
                        f"(consecutive_failures={state.consecutive_failures})"
                    )
                state.in_shedding_mode = True

        
        mark_delivery_done(msg_id)



subscriber_states: Dict[str, SubscriberState] = {}
subscriber_threads: Dict[str, threading.Thread] = {}
subscribers_lock = threading.Lock()


def refresh_subscribers() -> None:
    """Update subscriber threads from Redis registrations."""
    try:
        ids_in_redis = redis_client.smembers("subscribers")
    except Exception as exc:
        logger.error(f"Could not read subscriber list: {exc}")
        return

    with subscribers_lock:
        
        for sub_id in ids_in_redis:
            if sub_id not in subscriber_states:
                data = redis_client.hgetall(f"subscriber:{sub_id}")
                if not data:
                    continue
                state = SubscriberState(
                    subscriber_id=sub_id,
                    url=data["url"],
                    secret_key=data["secret_key"],
                )
                subscriber_states[sub_id] = state

                
                t = threading.Thread(
                    target=subscriber_worker,
                    args=(state, BACKPRESSURE_STRATEGY),
                    name=f"worker-{sub_id[:8]}",
                    daemon=True,
                )
                t.start()
                subscriber_threads[sub_id] = t
                logger.info(f"Added subscriber {sub_id} → {data['url']}")

        
        for sub_id in list(subscriber_states.keys()):
            if sub_id not in ids_in_redis:
                del subscriber_states[sub_id]
                del subscriber_threads[sub_id]
                logger.info(f"Removed subscriber {sub_id}")



def recover_pending_messages() -> None:
    """Reclaim unacknowledged messages after a crash."""
    try:
        pending = redis_client.xpending_range(
            STREAM_NAME, CONSUMER_GROUP, min="-", max="+", count=200
        )
    except Exception as exc:
        logger.warning(f"Could not read pending messages: {exc}")
        return

    if not pending:
        return

    logger.info(f"Recovering {len(pending)} pending messages from before crash...")

    for entry in pending:
        msg_id = entry["message_id"]
        
        try:
            claimed = redis_client.xclaim(
                STREAM_NAME,
                CONSUMER_GROUP,
                CONSUMER_NAME,
                min_idle_time=0,
                message_ids=[msg_id],
            )
        except Exception as exc:
            logger.error(f"xclaim failed for {msg_id}: {exc}")
            continue

        for m_id, m_data in claimed:
            if not m_data:
                continue
            try:
                event = json.loads(m_data["data"])
            except (json.JSONDecodeError, KeyError):
                redis_client.xack(STREAM_NAME, CONSUMER_GROUP, m_id)
                continue

            
            stored_req_id = redis_client.get(f"req_id:{m_id}")
            if stored_req_id:
                request_id = stored_req_id
            else:
                
                request_id = str(uuid.uuid4())
                redis_client.set(f"req_id:{m_id}", request_id, ex=86400)

            with subscribers_lock:
                target_states = list(subscriber_states.values())

            if not target_states:
                redis_client.xack(STREAM_NAME, CONSUMER_GROUP, m_id)
                continue

            with pending_lock:
                pending_acks[m_id] = len(target_states)

            for state in target_states:
                try:
                    state.delivery_queue.put_nowait((m_id, event, request_id))
                except queue.Full:
                    mark_delivery_done(m_id)



def metrics_logger() -> None:
    """Log queue depths periodically."""
    import os as _os
    _os.makedirs("/tmp/logs", exist_ok=True)

    while True:
        with subscribers_lock:
            depths = {
                sub_id: state.delivery_queue.qsize()
                for sub_id, state in subscriber_states.items()
            }
        entry = {
            "timestamp": time.time(),
            "strategy": BACKPRESSURE_STRATEGY,
            "depths": depths,
        }
        try:
            with open(QUEUE_DEPTH_LOG, "a") as fh:
                fh.write(json.dumps(entry) + "\n")
        except Exception:
            pass
        time.sleep(1.0)



def main() -> None:
    logger.info(
        f"Dispatcher starting | strategy={BACKPRESSURE_STRATEGY} "
        f"| consumer={CONSUMER_NAME}"
    )

    
    for attempt in range(30):
        try:
            redis_client.ping()
            logger.info("Redis connection established.")
            break
        except Exception:
            logger.info(f"Waiting for Redis... ({attempt + 1}/30)")
            time.sleep(2)

    
    try:
        
        redis_client.xgroup_create(
            STREAM_NAME, CONSUMER_GROUP, id="0", mkstream=True
        )
        logger.info(f"Consumer group '{CONSUMER_GROUP}' created.")
    except redis.exceptions.ResponseError as exc:
        if "BUSYGROUP" in str(exc):
            logger.info(f"Consumer group '{CONSUMER_GROUP}' already exists.")
        else:
            raise

    
    refresh_subscribers()
    recover_pending_messages()

    
    threading.Thread(target=metrics_logger, name="metrics-logger", daemon=True).start()

    last_refresh = time.time()
    admission_paused = False

    logger.info("Main dispatch loop running.")

    while True:
        
        if time.time() - last_refresh > 5:
            refresh_subscribers()
            last_refresh = time.time()

        
        if BACKPRESSURE_STRATEGY == "admission_control":
            with subscribers_lock:
                queue_sizes = [
                    s.delivery_queue.qsize()
                    for s in subscriber_states.values()
                ]
            max_depth = max(queue_sizes, default=0)

            if max_depth >= HIGH_WATER_MARK:
                if not admission_paused:
                    logger.warning(
                        f"Admission control: PAUSING reads "
                        f"(max queue depth={max_depth})"
                    )
                admission_paused = True

            if admission_paused and max_depth <= LOW_WATER_MARK:
                logger.info("Admission control: RESUMING reads.")
                admission_paused = False

            if admission_paused:
                time.sleep(0.1)
                continue

        
        with subscribers_lock:
            current_states = dict(subscriber_states)

        if not current_states:
            time.sleep(1)
            continue

        
        try:
            raw = redis_client.xreadgroup(
                CONSUMER_GROUP,
                CONSUMER_NAME,
                {STREAM_NAME: ">"},
                count=10,
                block=1000,
            )
        except redis.exceptions.RedisError as exc:
            logger.error(f"XREADGROUP error: {exc}")
            time.sleep(1)
            continue

        if not raw:
            continue

        for _stream_name, messages in raw:
            for msg_id, msg_data in messages:
                
                try:
                    event = json.loads(msg_data["data"])
                except (json.JSONDecodeError, KeyError) as exc:
                    logger.error(f"Skipping malformed message {msg_id}: {exc}")
                    redis_client.xack(STREAM_NAME, CONSUMER_GROUP, msg_id)
                    continue

                
                request_id = str(uuid.uuid4())
                redis_client.set(f"req_id:{msg_id}", request_id, ex=86400)
                priority = event.get("priority", "normal")

                
                targets = []
                for sub_id, state in current_states.items():

                    
                    if (
                        BACKPRESSURE_STRATEGY == "load_shedding"
                        and state.in_shedding_mode
                        and priority != "critical"
                    ):
                        logger.info(
                            f"[{sub_id[:8]}] SHEDDING {priority} event "
                            f"msg={msg_id}"
                        )
                        continue

                    targets.append(state)

                if not targets:
                    
                    redis_client.xack(STREAM_NAME, CONSUMER_GROUP, msg_id)
                    continue

                
                with pending_lock:
                    pending_acks[msg_id] = len(targets)

                
                queued_count = 0
                for state in targets:
                    try:
                        state.delivery_queue.put_nowait(
                            (msg_id, event, request_id)
                        )
                        queued_count += 1
                    except queue.Full:
                        
                        logger.warning(
                            f"[{state.subscriber_id}] Queue full, "
                            f"dropping msg={msg_id}"
                        )
                        mark_delivery_done(msg_id)

                logger.debug(
                    f"Dispatched msg={msg_id} to {queued_count}/{len(targets)} "
                    f"subscribers"
                )


if __name__ == "__main__":
    main()
