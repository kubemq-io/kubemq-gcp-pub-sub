"""KubeMQ Google Cloud Pub/Sub — delivery: ordering keys (Python / google-cloud-pubsub).

CreateTopic -> CreateSubscription(enable_message_ordering=True) ->
Publish three messages per ordering key with the publisher's message-ordering
enabled -> StreamingPull and assert each key's messages arrive in publish order.

This is an unmodified `google-cloud-pubsub` program. Setting PUBSUB_EMULATOR_HOST
makes the client clear credentials, skip Google auth, and dial insecure gRPC at
the KubeMQ Pub/Sub connector (default localhost:8085) — zero code changes.

Connector behaviour (spec S3.5 / docs/concepts/ordering-keys.md): with
`enable_message_ordering`, messages sharing an `ordering_key` are delivered in
publish order with AT MOST ONE in flight per key; keyless messages are unordered.
The key is carried across the wire as the reserved tag `_pubsub_ordering_key`.
"""

from __future__ import annotations

import os
import sys
import threading
import uuid

from google.api_core.exceptions import GoogleAPICallError
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.types import PublisherOptions

# --- Connection (PUBSUB_EMULATOR_HOST drop-in) -------------------------------
# Any project id; the connector parses but ignores the project segment.
PROJECT = os.environ.get("PUBSUB_PROJECT_ID", os.environ.get("GOOGLE_CLOUD_PROJECT", "my-project"))

# --- Unique-per-run resource names (channel-isolation rule, spec S9) ---------
# Bare ids would collide on the shared canonical channels pubsub.{t}/pubsub.sub.{s};
# a uuid8 suffix gives each run a private channel namespace so concurrent runs
# (other languages, or /examples + /burnin) never cross-talk.
RUN = uuid.uuid4().hex[:8]
TOPIC_ID = f"orders-{RUN}"
SUB_ID = f"sub-orders-{RUN}"

# Two independent ordering keys + one keyless message.
KEY_A = "cust-7"
KEY_B = "cust-9"


def check(condition: bool, message: str) -> None:
    """Assert an expected outcome; exit non-zero on failure (runnable proof)."""
    if condition:
        print(f"  [OK] {message}")
    else:
        print(f"  [FAIL] {message}", file=sys.stderr)
        raise SystemExit(1)


def main() -> None:
    print(f"=== Pub/Sub ordering keys — topic '{TOPIC_ID}' / sub '{SUB_ID}' ===")
    print(f"  emulator : {os.environ.get('PUBSUB_EMULATOR_HOST', '(unset!)')}")
    print(f"  project  : {PROJECT} (parsed but ignored by the connector)")
    print()

    # The publisher MUST enable message ordering as well (it serializes publishes
    # per key); enabling it only on the subscription is not enough.
    publisher = pubsub_v1.PublisherClient(
        publisher_options=PublisherOptions(enable_message_ordering=True)
    )
    subscriber = pubsub_v1.SubscriberClient()

    topic_path = publisher.topic_path(PROJECT, TOPIC_ID)
    sub_path = subscriber.subscription_path(PROJECT, SUB_ID)

    try:
        publisher.create_topic(request={"name": topic_path})
        print(f"CreateTopic            -> {topic_path}")

        subscriber.create_subscription(
            request={
                "name": sub_path,
                "topic": topic_path,
                "enable_message_ordering": True,
            }
        )
        print(f"CreateSubscription     -> {sub_path}  (enable_message_ordering=True)")
        print()

        # --- Publish: 3 ordered per key, interleaved, plus 1 keyless ---------
        plan = [
            (KEY_A, "A1"),
            (KEY_B, "B1"),
            (KEY_A, "A2"),
            ("", "keyless"),
            (KEY_B, "B2"),
            (KEY_A, "A3"),
            (KEY_B, "B3"),
        ]
        print("Publish (publisher ordering enabled; interleaved across two keys):")
        for key, body in plan:
            future = publisher.publish(topic_path, body.encode("utf-8"), ordering_key=key)
            msg_id = future.result(timeout=30)
            shown = key if key else "(keyless)"
            print(f"  publish body={body!r:<10} ordering_key={shown:<12} -> id={msg_id}")
        print()

        # --- StreamingPull: collect per-key delivery order ------------------
        per_key: dict[str, list[str]] = {KEY_A: [], KEY_B: [], "": []}
        seen_keys_lock = threading.Lock()
        done = threading.Event()
        expected_total = len(plan)
        received_total = 0

        def callback(message: pubsub_v1.subscriber.message.Message) -> None:
            nonlocal received_total
            # The connector strips the reserved _pubsub_* tags from attributes for
            # Pub/Sub clients; the ordering key is surfaced as message.ordering_key.
            key = message.ordering_key or ""
            body = message.data.decode("utf-8")
            with seen_keys_lock:
                per_key.setdefault(key, []).append(body)
                received_total += 1
                count = received_total
            shown = key if key else "(keyless)"
            print(f"  received body={body!r:<10} ordering_key={shown}")
            message.ack()
            if count >= expected_total:
                done.set()

        flow = pubsub_v1.types.FlowControl(max_messages=10)
        streaming_pull = subscriber.subscribe(sub_path, callback=callback, flow_control=flow)
        print("StreamingPull (one in flight per key; ack drives the next per-key message):")
        try:
            if not done.wait(timeout=30):
                streaming_pull.cancel()
                streaming_pull.result(timeout=10)
                check(False, f"received all {expected_total} messages within 30s")
        finally:
            streaming_pull.cancel()
            try:
                streaming_pull.result(timeout=10)
            except Exception:
                pass
        print()

        # --- Assertions ------------------------------------------------------
        print("Per-key delivery order:")
        print(f"  {KEY_A}: {per_key.get(KEY_A)}")
        print(f"  {KEY_B}: {per_key.get(KEY_B)}")
        print(f"  keyless: {per_key.get('', [])}")
        check(
            per_key.get(KEY_A) == ["A1", "A2", "A3"],
            f"key '{KEY_A}' delivered in publish order (A1,A2,A3)",
        )
        check(
            per_key.get(KEY_B) == ["B1", "B2", "B3"],
            f"key '{KEY_B}' delivered in publish order (B1,B2,B3)",
        )
        check(
            sorted(b for v in per_key.values() for b in v) == sorted(b for _, b in plan),
            "every published message was delivered exactly once",
        )
        print()
        print("PASS — ordering keys: per-key in-order delivery, one in flight per key.")

    finally:
        # Best-effort cleanup so re-runs start clean; never fail the run.
        for op, fn in (
            (
                "DeleteSubscription",
                lambda: subscriber.delete_subscription(request={"subscription": sub_path}),
            ),
            ("DeleteTopic", lambda: publisher.delete_topic(request={"topic": topic_path})),
        ):
            try:
                fn()
                print(f"cleanup: {op} ok")
            except GoogleAPICallError:
                pass


if __name__ == "__main__":
    main()
