"""
Load Test & Queue-Depth Logger.
Publishes events to the Producer to test the Dispatcher's performance.

Usage:
    python load_test.py [--events 200] [--rate 20]
"""

import argparse
import json
import sys
import time
import uuid

import httpx

PRODUCER_URL = "http://localhost:9001/events"


def parse_args():
    parser = argparse.ArgumentParser(description="Load test for webhook system")
    parser.add_argument("--events", type=int, default=200, help="Total events to send")
    parser.add_argument("--rate",   type=int, default=20,  help="Events per second")
    return parser.parse_args()


def publish_event(client: httpx.Client, i: int) -> dict:
    """Publish one event to the producer."""
    priority = "critical" if i % 5 == 0 else "normal"
    payload = {
        "event_type": "load.test",
        "payload": {
            "index":    i,
            "batch_id": str(uuid.uuid4())[:8],
        },
        "priority": priority,
    }
    try:
        resp = client.post(PRODUCER_URL, json=payload, timeout=5.0)
        return {"index": i, "status": resp.status_code, "msg_id": resp.json().get("message_id")}
    except Exception as exc:
        return {"index": i, "error": str(exc)}


def main():
    args = parse_args()
    interval = 1.0 / args.rate  # Seconds between publishes

    print(f"Starting load test: {args.events} events @ {args.rate}/s")
    print(f"Target: {PRODUCER_URL}")
    print(f"Press Ctrl+C to stop early.\n")

    results = []
    start = time.time()

    with httpx.Client() as client:
        try:
            client.get("http://localhost:9001/health", timeout=5).raise_for_status()
        except Exception as exc:
            print(f"ERROR: Producer not reachable: {exc}")
            print("Run 'docker compose up' first.")
            sys.exit(1)

        for i in range(1, args.events + 1):
            t0 = time.time()
            result = publish_event(client, i)
            results.append(result)

            status = result.get("status", "ERR")
            if i % 20 == 0 or i == 1:
                elapsed = time.time() - start
                print(f"  [{i:4d}/{args.events}] status={status}  elapsed={elapsed:.1f}s")

            sleep_time = interval - (time.time() - t0)
            if sleep_time > 0:
                time.sleep(sleep_time)

    elapsed = time.time() - start
    succeeded = sum(1 for r in results if r.get("status") == 201)
    failed    = len(results) - succeeded

    print(f"\nDone in {elapsed:.1f}s")
    print(f"  Published: {succeeded}/{args.events} events (failed: {failed})")
    print("\nThe dispatcher is processing events in the background.")
    print("Queue depths are being logged to logs/queue_depths.log")
    print(f"\nWaiting 30 seconds for dispatcher to finish processing...")

    for remaining in range(30, 0, -5):
        print(f"  {remaining}s remaining...")
        time.sleep(5)

    print("\nLoad test complete. Run 'python generate_plots.py' to generate charts.")


if __name__ == "__main__":
    main()
