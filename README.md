# Webhook Delivery System

A production-grade webhook dispatcher built with Python, FastAPI, and Redis Streams. It delivers events from a durable queue to subscriber endpoints with cryptographic HMAC signatures, idempotency guarantees, and three distinct backpressure strategies.

## Architecture

```
External Client
     │
     │ POST /events
     ▼
┌──────────────┐   XADD    ┌─────────────────────┐
│   Producer   │──────────▶│  Redis Stream        │
│  port 8001   │           │  "webhook_events"    │
└──────────────┘           └──────────┬───────────┘
                                      │ XREADGROUP
Administrator                         ▼
     │ POST /subscribers  ┌───────────────────────┐
     ▼                    │       Dispatcher       │
┌──────────────┐          │   (Python worker)      │
│  Admin API   │◀─────────│                        │
│  port 8002   │          │ Signs payload (HMAC)   │
│              │          │ Applies backpressure   │
│ Stores:      │          │ Retries on failure     │
│ url + secret │          │ XACK when delivered    │
└──────────────┘          └───────────┬────────────┘
                                      │ HTTP POST
                                      │ (5 signed headers)
                          ┌───────────▼────────────┐
                          │  Subscriber Simulator   │
                          │  ports 8000/8003/8004   │
                          │                         │
                          │ Validates HMAC signature│
                          │ Checks timestamp age    │
                          │ Deduplicates by UUID    │
                          │ (idempotency)           │
                          └─────────────────────────┘
```

## Services

| Service | Host Port | Description |
|---|---|---|
| `redis` | 6379 | Redis 7 with Streams support |
| `producer` | **9001** | FastAPI — accepts events, publishes to Redis Stream |
| `admin` | **9002** | FastAPI — manages subscriber registrations |
| `subscriber` | **9000** | FastAPI — fast profile (20 ms, 0 % errors) |
| `subscriber-medium` | **9003** | FastAPI — medium profile (200 ms, 5 % errors) |
| `subscriber-slow` | **9004** | FastAPI — slow profile (500 ms, 15 % errors) |
| `dispatcher` | — | Python worker — reads stream, signs, delivers |

## Quick Start

### Prerequisites

- Docker Desktop (or Docker Engine + Compose plugin)
- Python 3.11+ (for running `attack.py`, `load_test.py`, `generate_plots.py` locally)

### 1. Start everything

```bash
docker compose up --build
```

Wait until you see all services report `healthy`:

```bash
docker compose ps
```

### 2. Publish a test event

```bash
curl -s -X POST http://localhost:9001/events \
  -H "Content-Type: application/json" \
  -d '{
    "event_type": "order.created",
    "payload": {"order_id": "xyz-123", "amount": 99.99},
    "priority": "critical"
  }' | python -m json.tool
```

Expected response:
```json
{
  "status": "queued",
  "message_id": "1718000000000-0",
  "event_type": "order.created",
  "priority": "critical"
}
```

### 3. Verify delivery

```bash
# See processed events on the fast subscriber
curl http://localhost:9000/processed | python -m json.tool
```

### 4. Register a new subscriber

```bash
curl -s -X POST http://localhost:9002/subscribers \
  -H "Content-Type: application/json" \
  -d '{"url": "http://subscriber:8000/webhook"}' | python -m json.tool
```

### 5. List all subscribers

```bash
curl http://localhost:9002/subscribers | python -m json.tool
```

## Backpressure Strategies

Change the strategy by editing `docker-compose.yml` or by restarting the dispatcher with a new env var:

```bash
# Option A: edit docker-compose.yml
#   BACKPRESSURE_STRATEGY: "admission_control"
# Then:
docker compose up -d dispatcher

# Option B: one-liner restart
BACKPRESSURE_STRATEGY=load_shedding docker compose up -d dispatcher
```

| Value | Description |
|---|---|
| `rate_limiting` | Token bucket — each subscriber gets 10 tokens refilled at 2/s |
| `admission_control` | Stops reading from Redis when any queue exceeds 100 events |
| `load_shedding` | Drops normal-priority events after 3 consecutive failures |

## Security Headers

Every webhook POST includes five headers:

| Header | Example | Purpose |
|---|---|---|
| `X-Webhook-Signature` | `sha256=abc123...` | HMAC-SHA256 of the request body |
| `X-Webhook-Timestamp` | `1718000000` | Unix timestamp (replay protection) |
| `X-Request-ID` | `550e8400-...` | Stable UUID across retries (idempotency) |
| `X-Webhook-Version` | `v1` | Schema version |
| `X-Delivery-Attempt` | `1` | Retry counter (1-based) |

## Security Tests

```bash
# Install dependencies (if not already)
pip install httpx

# Run all 5 attack scenarios
python attack.py
```

Expected output:
```
  [✓ PASS] Tampered body
  [✓ PASS] Replay attack (stale timestamp)
  [✓ PASS] Missing X-Webhook-Signature
  [✓ PASS] Wrong X-Webhook-Version (v99)
  [✓ PASS] Request-ID flood (1000 same ID)

Results: 5/5 attacks correctly rejected
```

## Load Testing

```bash
# Run 200 events at 20/s
python load_test.py --events 200 --rate 20

# Generate queue-depth plots
pip install matplotlib
python generate_plots.py
# → plots/ directory contains rate_limiting.png, admission_control.png,
#   load_shedding.png, comparison.png
```

See [RESULTS.md](RESULTS.md) for detailed analysis.

## Crash Recovery Test

```bash
# 1. Publish 10 events
for i in $(seq 1 10); do
  curl -s -X POST http://localhost:9001/events \
    -H "Content-Type: application/json" \
    -d "{\"event_type\":\"crash.test\",\"payload\":{\"i\":$i},\"priority\":\"critical\"}"
done

# 2. Kill the dispatcher forcefully (simulates crash/power loss)
docker kill dispatcher

# 3. It restarts automatically (restart: always policy)
docker compose logs -f dispatcher

# 4. Verify all 10 events were delivered — no duplicates, no losses
curl http://localhost:9000/processed | python -m json.tool
```

## Project Structure

```
webhook-delivery-system/
├── docker-compose.yml        # Orchestrates all 7 services
├── .env.example              # Documents all environment variables
├── attack.py                 # 5 security exploit tests
├── load_test.py              # Drives load and waits for processing
├── generate_plots.py         # Reads queue-depth logs and plots with matplotlib
├── RESULTS.md                # Load test analysis with plots
├── SECURITY.md               # Security attack analysis
│
├── producer/                 # Event Producer (FastAPI, port 8001)
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py               # POST /events → XADD to Redis Stream
│
├── admin/                    # Admin API (FastAPI, port 8002)
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py               # CRUD for subscriber registrations
│
├── dispatcher/               # Dispatcher Worker (Python, no HTTP)
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py               # XREADGROUP → sign → deliver → XACK
│
├── subscriber/               # Subscriber Simulator (FastAPI, port 8000/8003/8004)
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py               # Validates HMAC, timestamp, idempotency
│
└── logs/                     # Volume-mounted; contains queue_depths.log
```

## How Each Core Requirement is Met

| Requirement | Implementation |
|---|---|
| Single `docker compose up` starts all services | `docker-compose.yml` with health checks and `depends_on` |
| POST /events publishes to Redis Stream | `producer/main.py` → `redis.xadd("webhook_events", ...)` |
| POST/GET/DELETE /subscribers | `admin/main.py` with Redis Hash + Set storage |
| Five required headers on every webhook | `dispatcher/main.py` `make_valid_headers()` function |
| HMAC-SHA256 signature verification | `subscriber/main.py` `_verify_hmac()` with `hmac.compare_digest` |
| Stale timestamp rejection (>5 min) | `subscriber/main.py` timestamp age check |
| Idempotency via X-Request-ID | `subscriber/main.py` Redis SET NX with 24h TTL |
| Three backpressure strategies | `dispatcher/main.py` selected by `BACKPRESSURE_STRATEGY` env var |
| Queue-depth plots in RESULTS.md | `generate_plots.py` + `RESULTS.md` |
| `attack.py` with 5 exploit scenarios | `attack.py` |
| Correct rejection of all attacks | `subscriber/main.py` validation pipeline |
| No event loss on dispatcher crash | Redis Consumer Group PEL + `recover_pending_messages()` |
| `.env.example` documents all variables | `.env.example` |
