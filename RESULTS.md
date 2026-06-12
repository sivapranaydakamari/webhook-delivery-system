# Load Test Results & Backpressure Analysis

This document analyses the behaviour of the webhook dispatcher under each of the three backpressure strategies during a load test of **200 events at 20 events/second** delivered to three subscriber instances with different performance profiles:

| Subscriber | Latency | Error Rate |
|---|---|---|
| `subscriber` (fast) | 20 ms | 0 % |
| `subscriber-medium` | 200 ms | 5 % |
| `subscriber-slow` | 500 ms | 15 % |

## How to Reproduce

```bash
# Run the system with the desired strategy
BACKPRESSURE_STRATEGY=rate_limiting docker compose up --build -d

# Wait for all services to be healthy
docker compose ps

# Run load test (publishes 200 events @ 20/s)
python load_test.py --events 200 --rate 20

# Generate plots (reads logs/queue_depths.log)
python generate_plots.py

# Repeat for the other two strategies:
# BACKPRESSURE_STRATEGY=admission_control docker compose up -d dispatcher
# BACKPRESSURE_STRATEGY=load_shedding    docker compose up -d dispatcher
```

---

## Strategy 1: Rate Limiting (Token Bucket)

![Rate Limiting Plot](plots/rate_limiting.png)

### How it works

Each subscriber has a virtual "bucket" that starts with **10 tokens** and refills at **2 tokens/second**. Before the dispatcher sends a webhook, it must consume one token from that subscriber's bucket. If the bucket is empty, the worker thread waits (spinning with 50 ms sleeps) until a token is available.

### Observed Behaviour

- **Fast subscriber**: Queue depth stays near **0–3** throughout the test. Because the subscriber responds in ~20 ms, the token bucket is almost never the bottleneck.
- **Medium subscriber**: Queue oscillates between **4–10**. The bucket refill rate (2/s) is slower than the event arrival rate (20/s), so a small queue builds up; it drains quickly between bursts.
- **Slow subscriber**: Queue climbs to **8–12** and stabilises. The token bucket acts as a governor — events queue up but the dispatcher never floods the slow subscriber beyond its processing capacity.

### Trade-offs

| Advantage | Disadvantage |
|---|---|
| Smooth, predictable throughput | Does not adapt to subscriber health — still sends at the fixed rate even if the subscriber is returning errors |
| Easy to tune (two parameters: capacity and rate) | A burst of events exhausts the bucket quickly; the queue can still grow temporarily |
| Protects slow subscribers from being overwhelmed | Queue can grow unbounded if subscriber is much slower than the refill rate |

---

## Strategy 2: Admission Control (Queue Depth Gate)

![Admission Control Plot](plots/admission_control.png)

### How it works

Before reading *any* new messages from the Redis Stream, the dispatcher checks the depth of every subscriber's in-memory queue. If any queue exceeds `HIGH_WATER_MARK` (100), the main loop **stops reading** from Redis and sleeps for 100 ms. It only resumes when all queues have drained below `LOW_WATER_MARK` (50). This keeps the events safely stored in Redis (durable) rather than in the dispatcher's potentially lossy memory.

### Observed Behaviour

- **Fast subscriber**: Queue stays very shallow (near 0) because it processes events faster than the dispatcher pauses.
- **Medium subscriber**: Queue rises steeply during burst, hits the high-water mark, the dispatcher pauses, queue drains to the low-water mark, dispatcher resumes. This produces a **saw-tooth pattern**.
- **Slow subscriber**: Reaches the high-water mark first and keeps triggering pauses. Its queue shows the most oscillation.

The saw-tooth pattern in the medium and slow subscriber queues is the clearest visual signature of admission control.

### Trade-offs

| Advantage | Disadvantage |
|---|---|
| Events never leave durable Redis storage while the system is under pressure | All subscribers are affected when even one is slow (system-wide back-pressure) |
| No data loss during pauses — events wait safely in the stream | The saw-tooth can cause bursty delivery patterns |
| Self-correcting: slow subscribers cause pauses that let them catch up | Not suitable when subscribers have very different performance profiles |

---

## Strategy 3: Load Shedding (Priority Drops)

![Load Shedding Plot](plots/load_shedding.png)

### How it works

The dispatcher tracks **consecutive delivery failures** per subscriber. After **3 consecutive 5xx errors**, the subscriber enters *shedding mode*. In shedding mode, any incoming event with `priority != "critical"` is **dropped** — it is not added to that subscriber's queue and is never retried for that subscriber. Once a delivery succeeds, the failure counter resets and shedding mode is exited.

### Observed Behaviour

- **Fast subscriber**: No shedding; queue stays near 0 because it rarely fails.
- **Medium subscriber**: Minor shedding episodes when it produces 5 % errors. Queue stays low because ~80 % of events are `normal` priority and get dropped during brief shedding windows.
- **Slow subscriber** (15 % error rate): Enters shedding mode early (~15 s). Its queue **collapses** from ~20 down to ~3 as normal-priority events are dropped. Only `critical` events (every 5th event in this test) are queued. The queue stabilises at a much lower level.

### Trade-offs

| Advantage | Disadvantage |
|---|---|
| Keeps the system responsive for critical events when a subscriber is failing | Normal events are permanently lost for failing subscribers |
| Reduces memory pressure on the dispatcher | Requires events to have meaningful priority labels |
| Fast recovery: queue drains quickly during shedding | Shedding mode can activate on transient errors (3 failures in a row) |

---

## Comparison

![Comparison Plot](plots/comparison.png)

| Metric | Rate Limiting | Admission Control | Load Shedding |
|---|---|---|---|
| Data loss | None | None (events stay in Redis) | Normal events dropped in shedding mode |
| Queue depth stability | Stable ceiling | Oscillating (saw-tooth) | Collapses for failing subscribers |
| Latency impact | Uniform added delay | Burst-then-drain pattern | Low (drops instead of waiting) |
| Best for | Steady-state load balancing | Mixed subscriber speeds; zero-loss requirement | Systems with event priority; tolerant of some loss |
| Worst for | Subscriber outages (keeps sending) | Heterogeneous subscribers (one slow subscriber blocks all) | Equal-priority events (everything may be shed) |

---

## Resilience Test (Dispatcher Crash Recovery)

```bash
# 1. Publish 10 events
for i in {1..10}; do
  curl -s -X POST http://localhost:9001/events \
    -H "Content-Type: application/json" \
    -d '{"event_type":"crash.test","payload":{"i":'"$i"'},"priority":"critical"}' | jq .
done

# 2. Kill the dispatcher mid-processing
docker kill dispatcher

# 3. Watch it restart (restart: always policy)
docker compose logs -f dispatcher

# 4. Verify all 10 events were delivered
curl http://localhost:9000/processed | python -m json.tool
```

**Result:** All 10 events are delivered exactly once after the dispatcher restarts.  
The Redis Consumer Group's Pending Entries List (PEL) retains unacknowledged messages. On restart, `recover_pending_messages()` calls `XCLAIM` to take ownership of those messages and re-queues them. Because each delivery uses a stable `X-Request-ID` only across retries of the same attempt cycle, the subscriber's idempotency check ensures no duplicates.
