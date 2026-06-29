"""KubeMQ Google Cloud Pub/Sub — advanced: seek to timestamp (Python / google-cloud-pubsub).

CreateTopic -> CreateSubscription -> Publish(N) -> Pull/Ack(all)
            -> Seek(timestamp checkpoint) -> Pull (replays the in-window tail)
            -> Seek(pre-window timestamp) -> Pull (clamps to earliest retained).

The topic is backed by a durable, replayable Events Store log (`pubsub.{t}`), so a
`Seek` to a timestamp rewinds the subscription's cursor and replays the topic log from
the first message at/after that time. A seek to a timestamp older than the earliest
retained message CLAMPS to the earliest retained message (it is NOT an error).

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
from datetime import datetime, timedelta, timezone

from google.api_core.exceptions import NotFound
from google.cloud import pubsub_v1
from google.protobuf.timestamp_pb2 import Timestamp
from google.pubsub_v1.types import SeekRequest, Subscription

PROJECT = os.environ.get("PUBSUB_PROJECT_ID", "my-project")
SUFFIX = uuid.uuid4().hex[:8]
TOPIC_ID = f"seekable-{SUFFIX}"
SUB_ID = f"sub-seekable-{SUFFIX}"


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
    print(f"== advanced/seek_timestamp — topic '{TOPIC_ID}', sub '{SUB_ID}' ==")
    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()
    topic_path = publisher.topic_path(PROJECT, TOPIC_ID)
    sub_path = subscriber.subscription_path(PROJECT, SUB_ID)

    try:
        publisher.create_topic(name=topic_path)
        print(f"CreateTopic -> {topic_path}  (Events Store log pubsub.{TOPIC_ID})")
        # retain_acked_messages keeps the topic log replayable after ack so a seek can
        # rewind past already-acknowledged messages.
        subscriber.create_subscription(
            request=Subscription(
                name=sub_path,
                topic=topic_path,
                retain_acked_messages=True,
                message_retention_duration={"seconds": 600},
            )
        )
        print(f"CreateSubscription -> {sub_path}  (Queue pubsub.sub.{SUB_ID}, retain_acked=true)")

        # Publish msg-1, take a checkpoint timestamp, then publish msg-2 + msg-3.
        # The checkpoint sits strictly between msg-1 and msg-2.
        publisher.publish(topic_path, b"msg-1").result(timeout=10)
        print("Publish -> msg-1")
        time.sleep(1.2)
        checkpoint = datetime.now(timezone.utc)
        print(f"checkpoint timestamp recorded -> {checkpoint.isoformat(timespec='seconds')}")
        time.sleep(1.2)
        publisher.publish(topic_path, b"msg-2").result(timeout=10)
        publisher.publish(topic_path, b"msg-3").result(timeout=10)
        print("Publish -> msg-2, msg-3")

        # 1) Initial drain: all three are delivered once.
        initial = drain(subscriber, sub_path)
        print(f"Pull (initial) -> {initial}")
        check(initial == ["msg-1", "msg-2", "msg-3"], "initial drain delivered all 3 in order")

        # 2) Seek to the checkpoint: replays only the tail published at/after it.
        ts = Timestamp()
        ts.FromDatetime(checkpoint)
        subscriber.seek(request=SeekRequest(subscription=sub_path, time=ts))
        print("Seek(timestamp=checkpoint) -> cursor rewound")
        replay = drain(subscriber, sub_path)
        print(f"Pull (after checkpoint seek) -> {replay}")
        check(replay == ["msg-2", "msg-3"], "seek-to-checkpoint replayed only msg-2, msg-3")

        # 3) Seek to a pre-window timestamp: clamps to the earliest retained message.
        pre_window = datetime.now(timezone.utc) - timedelta(days=365)
        past_ts = Timestamp()
        past_ts.FromDatetime(pre_window)
        subscriber.seek(request=SeekRequest(subscription=sub_path, time=past_ts))
        print("Seek(timestamp=now-365d) -> pre-window")
        clamped = drain(subscriber, sub_path)
        print(f"Pull (after pre-window seek) -> {clamped}")
        print("seek to pre-window timestamp -> clamped to earliest retained (not an error)")
        check(
            clamped == ["msg-1", "msg-2", "msg-3"],
            "pre-window seek clamped to earliest retained (all 3 replayed)",
        )

        print("\nPASS: timestamp seek replayed the in-window tail and clamped a pre-window seek.")
    finally:
        with contextlib.suppress(NotFound, Exception):
            subscriber.delete_subscription(subscription=sub_path)
        with contextlib.suppress(NotFound, Exception):
            publisher.delete_topic(topic=topic_path)
        sys.stdout.flush()


if __name__ == "__main__":
    main()
