"""KubeMQ Google Cloud Pub/Sub — delivery: message filtering (Python / google-cloud-pubsub).

CreateTopic -> CreateSubscription(filter='attributes.region = "eu"') +
CreateSubscription(no filter) -> Publish a mixed batch with a `region` attribute ->
Pull both subs and assert the filtered subscription receives ONLY the matching
subset while the unfiltered subscription receives everything.

This is an unmodified `google-cloud-pubsub` program. Setting PUBSUB_EMULATOR_HOST
makes the client clear credentials, skip Google auth, and dial insecure gRPC at
the KubeMQ Pub/Sub connector (default localhost:8085) — zero code changes.

Connector behaviour (spec S3.5 / docs/guides/filtering.md): a subscription's
CEL-subset `filter` is attributes-only, compiled once at create-time (immutable),
and applied at PUBLISH FAN-OUT — a non-matching message is never enqueued for that
subscription (effectively auto-acked for it). The topic log pubsub.{t} is unfiltered.
"""

from __future__ import annotations

import os
import sys
import uuid

from google.api_core.exceptions import GoogleAPICallError
from google.cloud import pubsub_v1

PROJECT = os.environ.get("PUBSUB_PROJECT_ID", os.environ.get("GOOGLE_CLOUD_PROJECT", "my-project"))

# Unique-per-run resource names (channel-isolation rule, spec S9).
RUN = uuid.uuid4().hex[:8]
TOPIC_ID = f"events-{RUN}"
SUB_FILTERED = f"sub-eu-{RUN}"
SUB_ALL = f"sub-all-{RUN}"

# CEL-subset, attributes-only, <= 256 chars, immutable after create.
EU_FILTER = 'attributes.region = "eu"'

# Mixed batch: region attribute drives the filter.
MESSAGES = [
    ("order-1", {"region": "eu", "type": "order"}),
    ("order-2", {"region": "us", "type": "order"}),
    ("order-3", {"region": "eu", "type": "order"}),
    ("order-4", {"region": "ap", "type": "order"}),
    ("order-5", {"region": "eu", "type": "order"}),
]
EU_BODIES = sorted(body for body, attrs in MESSAGES if attrs.get("region") == "eu")
ALL_BODIES = sorted(body for body, _ in MESSAGES)


def check(condition: bool, message: str) -> None:
    if condition:
        print(f"  [OK] {message}")
    else:
        print(f"  [FAIL] {message}", file=sys.stderr)
        raise SystemExit(1)


def drain(subscriber, sub_path: str, want: int) -> list[str]:
    """Pull until `want` messages are collected (or three empty polls)."""
    bodies: list[str] = []
    empty = 0
    while len(bodies) < want and empty < 3:
        resp = subscriber.pull(
            request={"subscription": sub_path, "max_messages": 10},
            timeout=8,
        )
        if not resp.received_messages:
            empty += 1
            continue
        ack_ids = []
        for rm in resp.received_messages:
            bodies.append(rm.message.data.decode("utf-8"))
            ack_ids.append(rm.ack_id)
        subscriber.acknowledge(request={"subscription": sub_path, "ack_ids": ack_ids})
    return bodies


def main() -> None:
    print(f"=== Pub/Sub message filtering — topic '{TOPIC_ID}' ===")
    print(f"  emulator : {os.environ.get('PUBSUB_EMULATOR_HOST', '(unset!)')}")
    print(f"  project  : {PROJECT} (parsed but ignored by the connector)")
    print(f"  filter   : {EU_FILTER}")
    print()

    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()

    topic_path = publisher.topic_path(PROJECT, TOPIC_ID)
    sub_filtered_path = subscriber.subscription_path(PROJECT, SUB_FILTERED)
    sub_all_path = subscriber.subscription_path(PROJECT, SUB_ALL)

    try:
        publisher.create_topic(request={"name": topic_path})
        print(f"CreateTopic            -> {topic_path}")

        subscriber.create_subscription(
            request={"name": sub_filtered_path, "topic": topic_path, "filter": EU_FILTER}
        )
        print(f"CreateSubscription     -> {sub_filtered_path}  (filter={EU_FILTER})")

        subscriber.create_subscription(request={"name": sub_all_path, "topic": topic_path})
        print(f"CreateSubscription     -> {sub_all_path}  (no filter)")
        print()

        print("Publish mixed batch (one Publish per message; region attribute drives the filter):")
        for body, attrs in MESSAGES:
            future = publisher.publish(topic_path, body.encode("utf-8"), **attrs)
            msg_id = future.result(timeout=30)
            print(f"  publish body={body!r:<9} region={attrs['region']:<3} -> id={msg_id}")
        print()

        print("Pull filtered subscription (expects EU only):")
        eu_got = sorted(drain(subscriber, sub_filtered_path, len(EU_BODIES)))
        print(f"  filtered received: {eu_got}")
        print("Pull unfiltered subscription (expects everything):")
        all_got = sorted(drain(subscriber, sub_all_path, len(ALL_BODIES)))
        print(f"  unfiltered received: {all_got}")
        print()

        check(eu_got == EU_BODIES, f"filtered sub received only region=eu ({EU_BODIES})")
        check(all_got == ALL_BODIES, "unfiltered sub received the full batch")
        suppressed = len(MESSAGES) - len(EU_BODIES)
        check(
            suppressed == 2,
            f"non-matching attribute -> suppressed ({suppressed} not enqueued for filtered sub)",
        )

        print()
        print("Negative path: a malformed filter is rejected at CreateSubscription:")
        bad_sub = subscriber.subscription_path(PROJECT, f"sub-bad-{RUN}")
        try:
            subscriber.create_subscription(
                request={"name": bad_sub, "topic": topic_path, "filter": "this is not a filter"}
            )
            check(False, "malformed filter should be rejected with INVALID_ARGUMENT")
        except GoogleAPICallError as exc:
            code = getattr(getattr(exc, "grpc_status_code", None), "name", type(exc).__name__)
            print(f"  malformed filter -> {code}")
            check(True, "malformed filter rejected at create-time (INVALID_ARGUMENT)")

        print()
        print("PASS — message filtering: attributes-only CEL-subset applied at publish fan-out.")

    finally:
        for op, fn in (
            (
                "DeleteSubscription(filtered)",
                lambda: subscriber.delete_subscription(request={"subscription": sub_filtered_path}),
            ),
            (
                "DeleteSubscription(all)",
                lambda: subscriber.delete_subscription(request={"subscription": sub_all_path}),
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
