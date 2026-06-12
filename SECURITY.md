# Security Analysis

This document describes the five security attacks tested by `attack.py` and explains how the Subscriber Simulator defends against each one.

---

## How to Run the Tests

```bash
# 1. Start the full system
docker compose up --build

# 2. Wait until all services are healthy (about 30 seconds)
docker compose ps

# 3. Run the attack suite from your host machine
python attack.py
```

---

## Attack Scenarios & Expected Results

### ① Tampered Body

**What the attacker does:**  
The dispatcher signs the *original* body with HMAC-SHA256 and sends it. An attacker intercepts the request and modifies the body (e.g., changes `amount: 100` to `amount: 99999`) while keeping the original signature header intact.

**How the subscriber defends:**  
On receipt, the subscriber recomputes the HMAC-SHA256 digest of the *received* body bytes using its secret key. Because the body has changed, the digest will differ from the one in `X-Webhook-Signature`. `hmac.compare_digest()` is used for a constant-time comparison that prevents timing attacks.

**Expected HTTP response:** `401 Unauthorized`

---

### ② Replay Attack (Stale Timestamp)

**What the attacker does:**  
The attacker records a valid, correctly-signed webhook and replays it hours or days later. The signature is still mathematically valid, but the request is old.

**How the subscriber defends:**  
The subscriber reads `X-Webhook-Timestamp` (a Unix timestamp) and compares it to the current time. If the difference exceeds `TIMESTAMP_TOLERANCE_SECONDS` (default: 300 s = 5 minutes), the request is rejected. The `attack.py` test uses a timestamp from 10 minutes ago.

**Expected HTTP response:** `400 Bad Request`

---

### ③ Missing Signature Header

**What the attacker does:**  
An attacker (or a misconfigured system) sends a POST to the webhook endpoint without the `X-Webhook-Signature` header at all.

**How the subscriber defends:**  
The subscriber checks for all five required headers at the very start of the validation pipeline. If any are missing, the request is rejected before any cryptographic computation is attempted.

**Expected HTTP response:** `400 Bad Request`

---

### ④ Wrong Webhook Version

**What the attacker does:**  
A request is sent with `X-Webhook-Version: v99`. This could indicate a schema version the subscriber has never seen, which might bypass validation logic written for a specific version.

**How the subscriber defends:**  
The subscriber explicitly checks that `X-Webhook-Version` equals `"v1"` (the only supported version). Any other value is rejected.

**Expected HTTP response:** `400 Bad Request`

---

### ⑤ Request-ID Flood (Idempotency Stress Test)

**What the attacker does:**  
The same valid, correctly-signed request is sent 1 000 times with an identical `X-Request-ID`. In a naive system, this would cause the subscriber to process the event 1 000 times — sending 1 000 emails, deducting a payment 1 000 times, etc.

**How the subscriber defends:**  
On the **first** receipt of a given `X-Request-ID`, the subscriber stores it in Redis:

```python
redis.set(f"idempotency:{request_id}", 1, ex=86400, nx=True)
```

The `NX` flag means "only set if Not eXists". If this returns `True`, the event is new and is processed. If it returns `False`, the key already exists and the request is silently accepted but **not re-processed**.

**Expected HTTP response:** `200 OK` for *all* 1 000 requests (first is processed, the rest are idempotent ignores). The event log file must contain exactly **one** entry for this `request_id`.

---

## Security Headers Summary

| Header | Purpose |
|---|---|
| `X-Webhook-Signature` | Proves the body came from a trusted source and was not tampered with |
| `X-Webhook-Timestamp` | Prevents replay attacks by bounding request age |
| `X-Request-ID` | Enables idempotent processing to prevent duplicate side-effects |
| `X-Webhook-Version` | Allows schema evolution without breaking existing subscribers |
| `X-Delivery-Attempt` | Lets subscribers log retry context for debugging |

## Security Limitations (for Production)

1. **Secret storage**: In this demo the secret key is stored in Redis in plaintext. Production systems should use a dedicated secrets manager (HashiCorp Vault, AWS Secrets Manager, etc.).
2. **Secret rotation**: There is no mechanism to rotate the signing secret without downtime. A production system would support multiple valid secrets during a rotation window.
3. **Rate limiting on the subscriber side**: The subscriber has no rate limiter on the `/webhook` endpoint itself. A DDoS attack could still exhaust its resources even if all requests are invalid.
4. **mTLS**: For higher assurance, mutual TLS could be used in addition to HMAC signing so that the transport layer also authenticates the caller.
