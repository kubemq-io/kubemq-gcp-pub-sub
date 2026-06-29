"""KubeMQ Google Cloud Pub/Sub — subscriptions: ack deadline & nack (Python / google-cloud-pubsub).

Lease lifecycle on a subscription, driven with unary ``Pull`` + ``ModifyAckDeadline``:
``Pull -> ModifyAckDeadline(>0) extend -> ModifyAckDeadline(0) nack -> redeliver -> Acknowledge``.

Every delivered message carries an opaque ``ack_id`` held under an ack-deadline lease.
``ModifyAckDeadline(ack_id, n>0)`` extends the lease (clamped to 10..600 s);
``ModifyAckDeadline(ack_id, 0)`` is an immediate **nack** that redelivers the message now; the
connector's 250 ms sweeper also redelivers any lease that expires un-acked. This example proves the
nack path: a pulled-but-nacked message comes back on the next pull, and a final ``Acknowledge``
drains it for good.

Uses ONLY the official ``google-cloud-pubsub`` client. With ``PUBSUB_EMULATOR_HOST`` set the SDK
clears credentials, skips Google auth, and dials insecure gRPC — no code changes. Resource ids are
uuid-suffixed so concurrent runs occupy distinct ``pubsub.{t}`` / ``pubsub.sub.{s}`` channels and do
not cross-talk; both resources are deleted on exit (best-effort).
"""

from __future__ import annotations

import os
import sys
import time
import uuid

from google.api_core.exceptions import GoogleAPICallError
from google.cloud import pubsub_v1

# Lease extension applied before the nack — must be within the valid 10..600 s window.
EXTEND_SECONDS = 30


def project_id() -> str:
    """Any id works — the connector parses but ignores the project segment."""
    return os.environ.get("PUBSUB_PROJECT_ID") or os.environ.get(
        "GOOGLE_CLOUD_PROJECT", "my-project"
    )


def unique(stem: str) -> str:
    """uuid-suffix a resource id so parallel runs never share a canonical channel.

    Each run then occupies distinct ``pubsub.{t}`` / ``pubsub.sub.{s}`` channels. Ids stay within
    the connector rules (3..255 chars, start with a letter, charset ``[A-Za-z0-9._~%+-]``); topic
    ids must not start with ``sub.``.
    """
    return f"{stem}-{uuid.uuid4().hex[:8]}"


def banner(title: str) -> None:
    print(f"=== {title} ===")
    print(f"  emulator : {os.environ.get('PUBSUB_EMULATOR_HOST', '(unset!)')}")
    print(f"  project  : {project_id()} (parsed but ignored by the connector)")
    print("  auth     : none — credentials cleared, insecure gRPC (emulator drop-in)")
    print()


def check(condition: bool, message: str) -> None:
    """Assert an expected outcome; exit non-zero on failure (examples are proofs)."""
    if condition:
        print(f"  [OK] {message}")
    else:
        print(f"  [FAIL] {message}", file=sys.stderr)
        raise SystemExit(1)


def pull_one(subscriber, sub_path):
    """Unary Pull of a single message; returns the ReceivedMessage or None if empty."""
    response = subscriber.pull(
        request={"subscription": sub_path, "max_messages": 1, "return_immediately": False},
        timeout=10,
    )
    msgs = response.received_messages
    return msgs[0] if msgs else None


def pull_with_retry(subscriber, sub_path, attempts=10, delay=0.3):
    """Poll for a (re)delivered message; the nacked copy reappears after a short backoff."""
    for _ in range(attempts):
        msg = pull_one(subscriber, sub_path)
        if msg is not None:
            return msg
        time.sleep(delay)
    return None


def main() -> None:
    banner("subscriptions: ack deadline extend + nack redelivery")

    project = project_id()
    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()

    topic_path = publisher.topic_path(project, unique("orders"))
    sub_path = subscriber.subscription_path(project, unique("sub-orders"))

    publisher.create_topic(name=topic_path)
    print(f"CreateTopic        -> {topic_path}")
    subscriber.create_subscription(
        request={"name": sub_path, "topic": topic_path, "ack_deadline_seconds": 10}
    )
    print(f"CreateSubscription -> {sub_path} (ack deadline 10 s)")

    try:
        payload = b"lease-me"
        publisher.publish(topic_path, payload, seq="0").result(timeout=15)
        print("Publish            -> 1 message to the topic log")

        # 1) First delivery — message is now leased under the ack deadline.
        first = pull_one(subscriber, sub_path)
        check(first is not None, "first Pull returns the message")
        print(f"Pull #1            -> body={first.message.data!r} ack_id={first.ack_id[:16]}...")

        # 2) Extend the lease. ModifyAckDeadline(>0) pushes the deadline out (clamped to 10..600 s)
        #    so a slow consumer keeps ownership instead of letting the sweeper redeliver.
        subscriber.modify_ack_deadline(
            request={
                "subscription": sub_path,
                "ack_ids": [first.ack_id],
                "ack_deadline_seconds": EXTEND_SECONDS,
            }
        )
        print(f"ModifyAckDeadline  -> +{EXTEND_SECONDS}s (lease extended, still owned)")

        # 3) Nack via ModifyAckDeadline(0): immediate redelivery instead of waiting out the lease.
        subscriber.modify_ack_deadline(
            request={"subscription": sub_path, "ack_ids": [first.ack_id], "ack_deadline_seconds": 0}
        )
        print("ModifyAckDeadline  -> 0 (nack: redeliver now)")

        # 4) The nacked message comes back on the next Pull.
        second = pull_with_retry(subscriber, sub_path)
        check(second is not None, "the nacked message is redelivered on a later Pull")
        check(second.message.data == payload, "redelivered body matches the original")
        print(f"Pull #2            -> body={second.message.data!r} ack_id={second.ack_id[:16]}...")

        # 5) Acknowledge for good — the lease is released and the message is gone.
        subscriber.acknowledge(request={"subscription": sub_path, "ack_ids": [second.ack_id]})
        print("Acknowledge        -> message acked")

        # 6) Prove the subscription is now empty (the ack removed it; no further redelivery).
        leftover = pull_one(subscriber, sub_path)
        check(leftover is None, "subscription is empty after the ack (no further redelivery)")

        print("\nAck-deadline / nack lifecycle complete — nack redelivered, ack drained.")
    finally:
        # Best-effort cleanup so re-runs start clean; never fail the run if already gone.
        for delete in (
            lambda: subscriber.delete_subscription(request={"subscription": sub_path}),
            lambda: publisher.delete_topic(request={"topic": topic_path}),
        ):
            try:
                delete()
            except GoogleAPICallError:
                pass
        subscriber.close()


if __name__ == "__main__":
    main()
