"""KubeMQ Google Cloud Pub/Sub — topics: batch publish (Python / google-cloud-pubsub).

CreateTopic -> CreateSubscription -> Publish(N, client-side batched) -> Pull(all) -> Acknowledge.

Demonstrates the client's automatic publish batching: the official
``google-cloud-pubsub`` ``PublisherClient`` buffers ``publish()`` calls and flushes
them in batches (tuned here via ``BatchSettings``). Each call still returns its own
future; resolving the futures in submission order yields the message ids "in request
order", and the connector validates the whole batch before enqueuing anything
(atomic publish; batch must be 1..1000).

Uses ONLY the official ``google-cloud-pubsub`` client driven via
``PUBSUB_EMULATOR_HOST`` (zero code changes). Resource names are uuid-suffixed
(channel-isolation rule) and deleted on exit (best-effort).
"""

from __future__ import annotations

import os
import sys
import uuid

from google.api_core.exceptions import GoogleAPICallError, NotFound
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.types import BatchSettings

BATCH_SIZE = 25  # well under the connector's 1000-message-per-batch ceiling.


def project_id() -> str:
    """Any id works — the connector parses but ignores the project segment."""
    return os.environ.get("PUBSUB_PROJECT_ID") or os.environ.get(
        "GOOGLE_CLOUD_PROJECT", "my-project"
    )


def rname(base: str) -> str:
    """uuid-suffix a resource id so parallel runs occupy private channels."""
    return f"{base}-{uuid.uuid4().hex[:8]}"


def banner(title: str) -> None:
    print(f"=== {title} ===")
    print(f"  emulator : {os.environ.get('PUBSUB_EMULATOR_HOST', '(unset!)')}")
    print(f"  project  : {project_id()} (parsed but ignored by the connector)")
    print("  auth     : none / insecure gRPC (emulator drop-in)")
    print()


def check(condition: bool, message: str) -> None:
    """Assert an expected outcome; exit non-zero on failure."""
    if condition:
        print(f"  [OK] {message}")
    else:
        print(f"  [FAIL] {message}", file=sys.stderr)
        raise SystemExit(1)


def main() -> None:
    proj = project_id()
    topic_id = rname("events")
    sub_id = rname("sub-events")
    banner(f"topics/batch_publish — topic '{topic_id}', batch of {BATCH_SIZE}")

    # Flush all BATCH_SIZE messages as a single client batch: raise the
    # max_messages threshold so the client does not split the batch.
    batch_settings = BatchSettings(max_messages=BATCH_SIZE, max_latency=1.0)
    publisher = pubsub_v1.PublisherClient(batch_settings=batch_settings)
    subscriber = pubsub_v1.SubscriberClient()
    topic_path = publisher.topic_path(proj, topic_id)
    sub_path = subscriber.subscription_path(proj, sub_id)

    try:
        created = publisher.create_topic(request={"name": topic_path})
        print(f"CreateTopic -> {created.name}")
        sub = subscriber.create_subscription(request={"name": sub_path, "topic": topic_path})
        print(f"CreateSubscription -> {sub.name}")

        # Submit BATCH_SIZE publishes; the client buffers and flushes them as one
        # batch. Each call returns a future; resolving in submission order gives
        # the ids in request order.
        futures = [
            publisher.publish(topic_path, f"event-{i:02d}".encode(), seq=str(i))
            for i in range(BATCH_SIZE)
        ]
        message_ids = [f.result(timeout=30) for f in futures]
        print(f"Publish -> {len(message_ids)} ids in request order")
        print(f"           first id={message_ids[0]}")
        print(f"           last  id={message_ids[-1]}")
        check(len(message_ids) == BATCH_SIZE, f"all {BATCH_SIZE} publishes returned an id")
        check(len(set(message_ids)) == BATCH_SIZE, "every message id is unique")

        # Drain the subscription: pull until we have all BATCH_SIZE messages.
        received: list = []
        while len(received) < BATCH_SIZE:
            resp = subscriber.pull(
                request={"subscription": sub_path, "max_messages": BATCH_SIZE},
                timeout=20,
            )
            if not resp.received_messages:
                break
            received.extend(resp.received_messages)
            subscriber.acknowledge(
                request={
                    "subscription": sub_path,
                    "ack_ids": [r.ack_id for r in resp.received_messages],
                }
            )
        print(f"Pull -> received and acked {len(received)} message(s)")
        check(len(received) == BATCH_SIZE, f"received all {BATCH_SIZE} published messages")

        bodies = {r.message.data.decode() for r in received}
        expected = {f"event-{i:02d}" for i in range(BATCH_SIZE)}
        check(bodies == expected, "every published body was delivered exactly once")

        # The publish-side id order is the submission order of the futures.
        seqs_in_id_order = [
            r.message.attributes["seq"]
            for r in sorted(received, key=lambda r: message_ids.index(r.message.message_id))
        ]
        check(
            seqs_in_id_order == [str(i) for i in range(BATCH_SIZE)],
            "publish ids are ordered by request submission order",
        )

        print(f"\nPASS — batch of {BATCH_SIZE} published and consumed exactly once.")
    finally:
        _cleanup(subscriber, sub_path)
        _cleanup(publisher, topic_path, is_topic=True)
        subscriber.close()


def _cleanup(client, path: str, is_topic: bool = False) -> None:
    try:
        if is_topic:
            client.delete_topic(request={"topic": path})
        else:
            client.delete_subscription(request={"subscription": path})
    except (NotFound, GoogleAPICallError):
        pass  # best-effort.


if __name__ == "__main__":
    main()
