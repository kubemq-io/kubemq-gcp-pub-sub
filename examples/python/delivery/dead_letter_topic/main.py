"""KubeMQ Google Cloud Pub/Sub — delivery: dead-letter topic (Python / google-cloud-pubsub).

CreateTopic(work) + CreateTopic(dead-letters) -> CreateSubscription on the DLQ
topic -> CreateSubscription on work with a DeadLetterPolicy
(dead_letter_topic + max_delivery_attempts=5) and a tight RetryPolicy ->
Publish one poison message -> pull-and-HOLD (never ack) so each ack-deadline
expiry raises the receive count -> once it exceeds max_delivery_attempts the
connector republishes the message to the dead-letter topic and acks the original.

This is an unmodified `google-cloud-pubsub` program. Setting PUBSUB_EMULATOR_HOST
makes the client clear credentials, skip Google auth, and dial insecure gRPC at
the KubeMQ Pub/Sub connector (default localhost:8085) — zero code changes.

Connector behaviour (spec S3.5 / docs/concepts/dead-letter-and-retry.md): the
dead-letter decision lives ONLY in the 250ms lease sweeper — each ACK-DEADLINE
EXPIRY increments the broker receive count; an explicit nack does NOT (Google
semantics: only expiry raises the count). When the count EXCEEDS
max_delivery_attempts and the sub names a dead_letter_topic, the sweeper
republishes the message (connector-level fan-out via pubsub.{dlt}) and acks the
original. max_delivery_attempts must be 5..100. (Mirrors the connector's own
TestIntegrationM2_DeadLetterAfterMaxAttempts.)
"""

from __future__ import annotations

import os
import sys
import time
import uuid

from google.api_core.exceptions import GoogleAPICallError
from google.cloud import pubsub_v1
from google.protobuf import duration_pb2
from google.pubsub_v1.types import DeadLetterPolicy, RetryPolicy

PROJECT = os.environ.get("PUBSUB_PROJECT_ID", os.environ.get("GOOGLE_CLOUD_PROJECT", "my-project"))

# Unique-per-run resource names (channel-isolation rule, spec S9).
RUN = uuid.uuid4().hex[:8]
WORK_TOPIC = f"work-{RUN}"
DLQ_TOPIC = f"dead-letters-{RUN}"
WORK_SUB = f"sub-work-{RUN}"
DLQ_SUB = f"sub-dlq-{RUN}"

# Must be in 5..100 (connector + Google floor); 0 would mean "unset".
MAX_DELIVERY_ATTEMPTS = 5
# Connector ack-deadline floor is 10s; each unacked lease expires after this.
ACK_DEADLINE_SECONDS = 10
POISON = "poison-pill"


def check(condition: bool, message: str) -> None:
    if condition:
        print(f"  [OK] {message}")
    else:
        print(f"  [FAIL] {message}", file=sys.stderr)
        raise SystemExit(1)


def main() -> None:
    print(f"=== Pub/Sub dead-letter topic — work '{WORK_TOPIC}' -> dlq '{DLQ_TOPIC}' ===")
    print(f"  emulator : {os.environ.get('PUBSUB_EMULATOR_HOST', '(unset!)')}")
    print(f"  project  : {PROJECT} (parsed but ignored by the connector)")
    print(f"  max_delivery_attempts : {MAX_DELIVERY_ATTEMPTS}")
    print("  note     : dead-lettering is driven by ack-deadline EXPIRY, not nack —")
    print(f"             each expiry waits ~{ACK_DEADLINE_SECONDS}s, so this runs ~60s.")
    print()

    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()

    work_topic_path = publisher.topic_path(PROJECT, WORK_TOPIC)
    dlq_topic_path = publisher.topic_path(PROJECT, DLQ_TOPIC)
    work_sub_path = subscriber.subscription_path(PROJECT, WORK_SUB)
    dlq_sub_path = subscriber.subscription_path(PROJECT, DLQ_SUB)

    try:
        publisher.create_topic(request={"name": work_topic_path})
        print(f"CreateTopic(work)      -> {work_topic_path}")
        publisher.create_topic(request={"name": dlq_topic_path})
        print(f"CreateTopic(dlq)       -> {dlq_topic_path}")

        # A subscription on the DLQ topic so we can observe the redriven message.
        subscriber.create_subscription(request={"name": dlq_sub_path, "topic": dlq_topic_path})
        print(f"CreateSubscription(dlq)-> {dlq_sub_path}")

        # The work subscription dead-letters to the DLQ topic after max attempts.
        # A tight RetryPolicy makes a swept lease re-arm and redeliver almost
        # immediately, so the loop is bounded by the ack deadline, not backoff.
        subscriber.create_subscription(
            request={
                "name": work_sub_path,
                "topic": work_topic_path,
                "ack_deadline_seconds": ACK_DEADLINE_SECONDS,
                "dead_letter_policy": DeadLetterPolicy(
                    dead_letter_topic=dlq_topic_path,
                    max_delivery_attempts=MAX_DELIVERY_ATTEMPTS,
                ),
                "retry_policy": RetryPolicy(
                    minimum_backoff=duration_pb2.Duration(nanos=10_000_000),  # 10ms
                    maximum_backoff=duration_pb2.Duration(nanos=50_000_000),  # 50ms
                ),
            }
        )
        print(
            f"CreateSubscription(work)-> {work_sub_path}  "
            f"(dead_letter_topic={DLQ_TOPIC}, max_delivery_attempts={MAX_DELIVERY_ATTEMPTS})"
        )
        print()

        publisher.publish(work_topic_path, POISON.encode("utf-8")).result(timeout=30)
        print(f"Publish -> {POISON!r}")
        print()

        # Drive redelivery via natural ack-deadline expiry: Pull leases the
        # message under the ack deadline; we deliberately NEVER ack it, so the
        # connector's lease sweeper expires the lease and raises the receive
        # count. Once the count exceeds max_delivery_attempts the sweeper moves
        # it to the DLQ topic. (An explicit nack would not raise the count.)
        print("Driving redelivery (Pull then hold unacked; each expiry raises the receive count):")
        dlq_body = None

        def poll_dlq() -> str | None:
            dresp = subscriber.pull(
                request={"subscription": dlq_sub_path, "max_messages": 1}, timeout=2
            )
            if dresp.received_messages:
                drm = dresp.received_messages[0]
                subscriber.acknowledge(
                    request={"subscription": dlq_sub_path, "ack_ids": [drm.ack_id]}
                )
                return drm.message.data.decode("utf-8")
            return None

        # Bound: ~7 expiry cycles of (ack deadline + slack). max_delivery_attempts=5,
        # so the message dead-letters on the cycle whose receive count exceeds 5.
        deadline = time.monotonic() + (ACK_DEADLINE_SECONDS + 2) * 8
        attempt = 0
        while dlq_body is None and time.monotonic() < deadline:
            # Lease the message (never ack); its lease will expire after the ack
            # deadline, and the sweeper raises the receive count on that expiry.
            resp = subscriber.pull(
                request={"subscription": work_sub_path, "max_messages": 1}, timeout=4
            )
            if resp.received_messages:
                attempt += 1
                rm = resp.received_messages[0]
                print(
                    f"  pull {attempt}: leased (delivery_attempt={rm.delivery_attempt}) "
                    f"-> hold unacked, let the lease expire"
                )
            # Wait just past the ack deadline for the lease to expire + sweeper to
            # redeliver/dead-letter, checking the DLQ cheaply as we go.
            cycle_end = time.monotonic() + ACK_DEADLINE_SECONDS + 2
            while dlq_body is None and time.monotonic() < cycle_end:
                dlq_body = poll_dlq()
                if dlq_body is not None:
                    break
                time.sleep(0.5)
        print()

        check(dlq_body is not None, "poison message landed on the dead-letter topic")
        print(f"  DLQ message body={dlq_body!r}")
        check(dlq_body == POISON, "DLQ holds the original body")
        check(attempt >= 1, "the source subscription delivered the message before dead-lettering")

        # The source subscription is now drained (the original was acked off it).
        drained = subscriber.pull(
            request={"subscription": work_sub_path, "max_messages": 1}, timeout=3
        )
        check(
            not drained.received_messages,
            "work subscription drained (original acked off the source)",
        )

        print()
        print("PASS — dead-letter topic: receive count over max_delivery_attempts redrives to DLQ.")

    finally:
        for op, fn in (
            (
                "DeleteSubscription(work)",
                lambda: subscriber.delete_subscription(request={"subscription": work_sub_path}),
            ),
            (
                "DeleteSubscription(dlq)",
                lambda: subscriber.delete_subscription(request={"subscription": dlq_sub_path}),
            ),
            (
                "DeleteTopic(work)",
                lambda: publisher.delete_topic(request={"topic": work_topic_path}),
            ),
            ("DeleteTopic(dlq)", lambda: publisher.delete_topic(request={"topic": dlq_topic_path})),
        ):
            try:
                fn()
                print(f"cleanup: {op} ok")
            except GoogleAPICallError:
                pass


if __name__ == "__main__":
    main()
