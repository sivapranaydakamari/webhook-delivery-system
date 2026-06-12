"""
Security Test Suite.
Tests five security exploits against the Subscriber Simulator to ensure they are correctly rejected.

Usage:
    python attack.py (after 'docker compose up' is healthy)
"""

import hashlib
import hmac
import json
import sys
import time
import uuid

import httpx

ADMIN_URL      = "http://localhost:9002"
SUBSCRIBER_URL = "http://localhost:9000/webhook"

TARGET_URL = "http://subscriber:8000/webhook"
TARGET_URL_EXTERNAL = "http://localhost:9000/webhook"


def get_secret_for_subscriber() -> tuple[str, str]:
    """Fetch the registered subscriber_id and secret from the Admin API."""
    resp = httpx.get(f"{ADMIN_URL}/subscribers", timeout=10)
    resp.raise_for_status()
    data = resp.json()

    subscribers = data.get("subscribers", [])
    if not subscribers:
        print("ERROR: No subscribers registered. Is the system running?")
        sys.exit(1)

    for sub in subscribers:
        if "subscriber" in sub.get("url", ""):
            return sub["subscriber_id"], sub["secret_key"]

    sub = subscribers[0]
    return sub["subscriber_id"], sub["secret_key"]


def make_signature(body_bytes: bytes, secret: str) -> str:
    """Generate the HMAC-SHA256 signature."""
    digest = hmac.new(
        secret.encode("utf-8"),
        msg=body_bytes,
        digestmod=hashlib.sha256,
    ).hexdigest()
    return f"sha256={digest}"


def make_valid_headers(
    body_bytes: bytes,
    secret: str,
    request_id: str = None,
    timestamp: int  = None,
    version: str    = "v1",
    attempt: int    = 1,
) -> dict:
    """Create valid headers for the request."""
    if request_id is None:
        request_id = str(uuid.uuid4())
    if timestamp is None:
        timestamp = int(time.time())
    return {
        "Content-Type":        "application/json",
        "X-Webhook-Signature": make_signature(body_bytes, secret),
        "X-Webhook-Timestamp": str(timestamp),
        "X-Request-ID":        request_id,
        "X-Webhook-Version":   version,
        "X-Delivery-Attempt":  str(attempt),
    }


def attack_1_tampered_body(secret: str) -> dict:
    """Send a tampered body after signing to test digest validation (expects 401)."""
    original_body = json.dumps(
        {"event_type": "test.event", "payload": {"amount": 100}, "priority": "normal"}
    ).encode()
    tampered_body = json.dumps(
        {"event_type": "test.event", "payload": {"amount": 99999}, "priority": "normal"}
    ).encode()

    headers = make_valid_headers(original_body, secret)
    resp = httpx.post(TARGET_URL_EXTERNAL, content=tampered_body, headers=headers, timeout=10)
    return {"attack": "Tampered body", "status": resp.status_code,
            "expected": "401", "pass": resp.status_code == 401}


def attack_2_replay(secret: str) -> dict:
    """Send a request with a stale timestamp to test replay protection (expects 400)."""
    body = json.dumps(
        {"event_type": "replay.attack", "payload": {}, "priority": "normal"}
    ).encode()
    stale_timestamp = int(time.time()) - 600
    headers = make_valid_headers(body, secret, timestamp=stale_timestamp)
    resp = httpx.post(TARGET_URL_EXTERNAL, content=body, headers=headers, timeout=10)
    return {"attack": "Replay attack (stale timestamp)", "status": resp.status_code,
            "expected": "400", "pass": resp.status_code == 400}


def attack_3_missing_signature(secret: str) -> dict:
    """Omit the signature header to ensure it's required (expects 400)."""
    body = json.dumps(
        {"event_type": "no.signature", "payload": {}, "priority": "normal"}
    ).encode()
    headers = make_valid_headers(body, secret)
    del headers["X-Webhook-Signature"]

    resp = httpx.post(TARGET_URL_EXTERNAL, content=body, headers=headers, timeout=10)
    return {"attack": "Missing X-Webhook-Signature", "status": resp.status_code,
            "expected": "400", "pass": resp.status_code == 400}


def attack_4_wrong_version(secret: str) -> dict:
    """Send an unsupported webhook version (expects 400)."""
    body = json.dumps(
        {"event_type": "version.attack", "payload": {}, "priority": "normal"}
    ).encode()
    headers = make_valid_headers(body, secret, version="v99")
    resp = httpx.post(TARGET_URL_EXTERNAL, content=body, headers=headers, timeout=10)
    return {"attack": "Wrong X-Webhook-Version (v99)", "status": resp.status_code,
            "expected": "400", "pass": resp.status_code == 400}


def attack_5_request_id_flood(secret: str) -> dict:
    """Flood the endpoint with the same request ID to test idempotency."""
    body = json.dumps(
        {"event_type": "flood.test", "payload": {"flood": True}, "priority": "normal"}
    ).encode()
    shared_request_id = str(uuid.uuid4())

    statuses = []
    for i in range(1000):
        headers = make_valid_headers(body, secret, request_id=shared_request_id)
        try:
            resp = httpx.post(
                TARGET_URL_EXTERNAL, content=body, headers=headers, timeout=10
            )
            statuses.append(resp.status_code)
        except Exception:
            statuses.append(0)

    all_200 = all(s == 200 for s in statuses)
    return {
        "attack":   "Request-ID flood (1000 same ID)",
        "status":   f"All 200: {all_200}  |  Distinct statuses: {set(statuses)}",
        "expected": "All 200 (first=processed, rest=idempotent ignore)",
        "pass":     all_200,
    }


def main():
    print("=" * 60)
    print("  Webhook Security Attack Test Suite")
    print("=" * 60)

    print(f"\nFetching subscriber secret from {ADMIN_URL}...")
    try:
        sub_id, secret = get_secret_for_subscriber()
        print(f"Found subscriber: {sub_id}")
    except Exception as exc:
        print(f"ERROR: Could not connect to Admin API: {exc}")
        print("Make sure all services are running: docker compose up")
        sys.exit(1)

    print(f"\nRunning attacks against {TARGET_URL_EXTERNAL}\n")
    print("-" * 60)

    attacks = [
        attack_1_tampered_body,
        attack_2_replay,
        attack_3_missing_signature,
        attack_4_wrong_version,
        attack_5_request_id_flood,
    ]

    results = []
    for attack_fn in attacks:
        result = attack_fn(secret)
        results.append(result)
        status_str = str(result["status"])
        icon = "✓ PASS" if result["pass"] else "✗ FAIL"
        print(f"  [{icon}] {result['attack']}")
        print(f"           Got: {status_str}  |  Expected: {result['expected']}")
        print()

    print("-" * 60)
    passed = sum(1 for r in results if r["pass"])
    print(f"\nResults: {passed}/{len(results)} attacks correctly rejected\n")

    if passed == len(results):
        print("All security checks PASSED. The subscriber is secure!")
        sys.exit(0)
    else:
        print("Some checks FAILED. Review the subscriber implementation.")
        sys.exit(1)


if __name__ == "__main__":
    main()
