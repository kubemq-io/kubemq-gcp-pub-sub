"""KubeMQ Google Cloud Pub/Sub — advanced: snapshots (Python / google-cloud-pubsub).

CreateTopic -> CreateSubscription -> CreateSnapshot (capture cursor)
            -> Publish(N) -> Pull/Ack(all) -> Seek(snapshot) -> Pull (replays from cursor)
            -> CreateSnapshot on a detached subscription -> FAILED_PRECONDITION (negative path).

A snapshot captures a subscription's current cursor as a registry record so you can seek
back to it later WITHOUT knowing an exact timestamp. Seeking to the snapshot rewinds the
subscription to the captured position and replays the topic log from there.

Uses ONLY the official `google-cloud-pubsub` client. Resource ids are uuid-suffixed so
concurrent runs occupy distinct `pubsub.{t}` / `pubsub.sub.{s}` channels and never
cross-talk. Exits 0 on success, non-zero on any failed assertion.
"""

from __future__ import annotations

import contextlib
import os
import sys
import time
import uuid

from google.api_core.exceptions import FailedPrecondition, NotFound
from google.cloud import pubsub_v1
from google.pubsub_v1.types import CreateSnapshotRequest, SeekRequest, Subscription

PROJECT = os.environ.get("PUBSUB_PROJECT_ID", "my-project")
SUFFIX = uuid.uuid4().hex[:8]
TOPIC_ID = f"snapped-{SUFFIX}"
SUB_ID = f"sub-snapped-{SUFFIX}"
SNAPSHOT_ID = f"snap-{SUFFIX}"
DETACHED_SUB_ID = f"sub-detached-{SUFFIX}"
DETACHED_SNAPSHOT_ID = f"snap-detached-{SUFFIX}"


def check(condition: bool, message: str) -> None:
    """Assert an expected outcome, printing an [OK] line or exiting non-zero."""
    if condition:
        print(f"[OK] {message}")
    else:
        print(f"[FAIL] {message}")
        raise SystemExit(1)


def drain(
    subscriber: pubsub_v1.SubscriberClient, sub_path: str, deadline: float = 6.0
) -> list[str]:
    """Pull + ack everything currently available, returning the message bodies in order."""
    bodies: list[str] = []
    end = time.time() + deadline
    while time.time() < end:
        response = subscriber.pull(request={"subscription": sub_path, "max_messages": 10})
        if not response.received_messages:
            time.sleep(0.3)
            continue
        ack_ids = []
        for received in response.received_messages:
            bodies.append(received.message.data.decode())
            ack_ids.append(received.ack_id)
        subscriber.acknowledge(request={"subscription": sub_path, "ack_ids": ack_ids})
    return bodies


def main() -> None:
    print(
        f"== advanced/snapshots — topic '{TOPIC_ID}', sub '{SUB_ID}', snapshot '{SNAPSHOT_ID}' =="
    )
    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()
    topic_path = publisher.topic_path(PROJECT, TOPIC_ID)
    sub_path = subscriber.subscription_path(PROJECT, SUB_ID)
    snapshot_path = subscriber.snapshot_path(PROJECT, SNAPSHOT_ID)
    detached_sub_path = subscriber.subscription_path(PROJECT, DETACHED_SUB_ID)
    detached_snapshot_path = subscriber.snapshot_path(PROJECT, DETACHED_SNAPSHOT_ID)

    try:
        publisher.create_topic(name=topic_path)
        print(f"CreateTopic -> {topic_path}  (Events Store log pubsub.{TOPIC_ID})")
        subscriber.create_subscription(
            request=Subscription(
                name=sub_path,
                topic=topic_path,
                retain_acked_messages=True,
                message_retention_duration={"seconds": 600},
            )
        )
        print(f"CreateSubscription -> {sub_path}  (Queue pubsub.sub.{SUB_ID})")

        # Capture the cursor BEFORE publishing — the snapshot pins this position.
        subscriber.create_snapshot(
            request=CreateSnapshotRequest(name=snapshot_path, subscription=sub_path)
        )
        print(f"CreateSnapshot -> {snapshot_path}  (cursor captured at current position)")

        # Publish three orders and consume them all; the cursor advances past them.
        for i in range(1, 4):
            publisher.publish(topic_path, f"order-{i}".encode()).result(timeout=10)
        print("Publish -> order-1, order-2, order-3")

        consumed = drain(subscriber, sub_path)
        print(f"Pull (initial) -> {consumed}")
        check(consumed == ["order-1", "order-2", "order-3"], "initial drain delivered all 3 orders")

        empty = drain(subscriber, sub_path, deadline=2.0)
        print(f"Pull (drain check) -> {empty}")
        check(empty == [], "subscription is empty after acking all 3")

        # Seek back to the snapshot: the cursor rewinds and the orders replay.
        subscriber.seek(request=SeekRequest(subscription=sub_path, snapshot=snapshot_path))
        print(f"Seek(snapshot={SNAPSHOT_ID}) -> cursor rewound to captured position")
        replay = drain(subscriber, sub_path)
        print(f"Pull (after seek-to-snapshot) -> {replay}")
        check(
            replay == ["order-1", "order-2", "order-3"],
            "seek-to-snapshot replayed all 3 orders from the captured cursor",
        )

        # Negative path: snapshotting a detached subscription -> FAILED_PRECONDITION.
        subscriber.create_subscription(
            request=Subscription(name=detached_sub_path, topic=topic_path)
        )
        print(f"CreateSubscription -> {detached_sub_path}  (will be detached)")
        publisher.delete_topic(topic=topic_path)
        print(f"DeleteTopic -> {topic_path}  (detaches its subscriptions)")
        try:
            subscriber.create_snapshot(
                request=CreateSnapshotRequest(
                    name=detached_snapshot_path, subscription=detached_sub_path
                )
            )
            print("[FAIL] snapshot of detached subscription unexpectedly succeeded")
            raise SystemExit(1)
        except FailedPrecondition:
            print("CreateSnapshot(detached sub) -> FAILED_PRECONDITION")
            check(True, "snapshot of a detached subscription rejected FAILED_PRECONDITION")

        print("\nPASS: snapshot captured the cursor, seek-to-snapshot replayed, detached rejected.")
    finally:
        with contextlib.suppress(NotFound, Exception):
            subscriber.delete_snapshot(snapshot=snapshot_path)
        with contextlib.suppress(NotFound, Exception):
            subscriber.delete_subscription(subscription=sub_path)
        with contextlib.suppress(NotFound, Exception):
            subscriber.delete_subscription(subscription=detached_sub_path)
        with contextlib.suppress(NotFound, Exception):
            publisher.delete_topic(topic=topic_path)
        sys.stdout.flush()


if __name__ == "__main__":
    main()
