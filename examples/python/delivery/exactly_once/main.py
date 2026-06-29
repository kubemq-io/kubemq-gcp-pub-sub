"""KubeMQ Google Cloud Pub/Sub — delivery: exactly-once (Python / google-cloud-pubsub).

CreateTopic -> CreateSubscription(enable_exactly_once_delivery=True) -> Publish ->
(1) StreamingPull and confirm the ack is DURABLE by awaiting the ack-result future
    (`message.ack_with_response().result()`); and
(2) demonstrate the unary stale-ack contract: re-acking an already-used ack_id
    returns FAILED_PRECONDITION with ErrorInfo(PERMANENT_FAILURE_INVALID_ACK_ID),
    which client libraries map to a permanent ack failure (NOT a literal
    INVALID_ARGUMENT).

This is an unmodified `google-cloud-pubsub` program. Setting PUBSUB_EMULATOR_HOST
makes the client clear credentials, skip Google auth, and dial insecure gRPC at
the KubeMQ Pub/Sub connector (default localhost:8085) — zero code changes.

Connector behaviour (spec S3.5 / docs/concepts/exactly-once.md): with
enable_exactly_once_delivery, StreamingPull returns Acknowledge/ModifyAckDeadline
confirmations (durable acks in `ack_ids`; expired ids in `invalid_ack_ids`;
transient failures in `temporary_failed_ack_ids`) and a unary Acknowledge on a
stale id returns FAILED_PRECONDITION + ErrorInfo(PERMANENT_FAILURE_INVALID_ACK_ID).
Exactly-once is NODE-LOCAL: an ack_id minted on one node is invalid on another —
pin a subscription's StreamingPull to one node (sticky LB) in a cluster.
"""

from __future__ import annotations

import os
import sys
import threading
import uuid

from google.api_core import exceptions as gexc
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.subscriber.exceptions import AcknowledgeError

PROJECT = os.environ.get("PUBSUB_PROJECT_ID", os.environ.get("GOOGLE_CLOUD_PROJECT", "my-project"))

# Unique-per-run resource names (channel-isolation rule, spec S9).
RUN = uuid.uuid4().hex[:8]
TOPIC_ID = f"orders-{RUN}"
SUB_STREAM = f"sub-eo-stream-{RUN}"
SUB_UNARY = f"sub-eo-unary-{RUN}"

STREAM_BODY = "exactly-once-streaming"
UNARY_BODY = "exactly-once-unary"


def check(condition: bool, message: str) -> None:
    if condition:
        print(f"  [OK] {message}")
    else:
        print(f"  [FAIL] {message}", file=sys.stderr)
        raise SystemExit(1)


def main() -> None:
    print(f"=== Pub/Sub exactly-once — topic '{TOPIC_ID}' ===")
    print(f"  emulator : {os.environ.get('PUBSUB_EMULATOR_HOST', '(unset!)')}")
    print(f"  project  : {PROJECT} (parsed but ignored by the connector)")
    print()

    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()

    topic_path = publisher.topic_path(PROJECT, TOPIC_ID)
    stream_sub_path = subscriber.subscription_path(PROJECT, SUB_STREAM)
    unary_sub_path = subscriber.subscription_path(PROJECT, SUB_UNARY)

    try:
        publisher.create_topic(request={"name": topic_path})
        print(f"CreateTopic            -> {topic_path}")

        for path, sid in ((stream_sub_path, SUB_STREAM), (unary_sub_path, SUB_UNARY)):
            subscriber.create_subscription(
                request={
                    "name": path,
                    "topic": topic_path,
                    "enable_exactly_once_delivery": True,
                }
            )
            print(f"CreateSubscription     -> {path}  (enable_exactly_once_delivery=True)")
        print()

        # --- Part 1: StreamingPull, confirmed (durable) ack ------------------
        print("Part 1 — StreamingPull: await the ack-result future to confirm a DURABLE ack")
        publisher.publish(topic_path, STREAM_BODY.encode("utf-8")).result(timeout=30)
        print(f"  Publish -> {STREAM_BODY!r}")

        confirmed = threading.Event()
        ack_failed: list[str] = []
        received_body: list[str] = []

        def callback(message: pubsub_v1.subscriber.message.Message) -> None:
            received_body.append(message.data.decode("utf-8"))
            # Exactly-once: ack returns a FUTURE; awaiting its result confirms the
            # ack took effect (durable). A permanent failure raises AcknowledgeError.
            ack_future = message.ack_with_response()
            try:
                ack_future.result(timeout=20)
                print("  ack confirmed durable (ack_with_response().result() returned)")
                confirmed.set()
            except AcknowledgeError as exc:
                ack_failed.append(str(exc.error_code))
                confirmed.set()

        flow = pubsub_v1.types.FlowControl(max_messages=1)
        streaming_pull = subscriber.subscribe(stream_sub_path, callback=callback, flow_control=flow)
        try:
            if not confirmed.wait(timeout=30):
                check(False, "StreamingPull delivered + confirmed the ack within 30s")
        finally:
            streaming_pull.cancel()
            try:
                streaming_pull.result(timeout=10)
            except Exception:
                pass

        check(received_body == [STREAM_BODY], "StreamingPull delivered the message exactly once")
        check(not ack_failed, "ack confirmation succeeded (durable, no AcknowledgeError)")
        print()

        # --- Part 2: unary stale-ack contract --------------------------------
        print("Part 2 — unary Acknowledge on a stale ack_id -> FAILED_PRECONDITION + ErrorInfo")
        publisher.publish(topic_path, UNARY_BODY.encode("utf-8")).result(timeout=30)
        print(f"  Publish -> {UNARY_BODY!r}")

        resp = subscriber.pull(
            request={"subscription": unary_sub_path, "max_messages": 1}, timeout=8
        )
        check(bool(resp.received_messages), "unary Pull delivered the message")
        ack_id = resp.received_messages[0].ack_id

        subscriber.acknowledge(request={"subscription": unary_sub_path, "ack_ids": [ack_id]})
        print("  first Acknowledge -> ok (durable)")

        # Re-acking the now-consumed ack_id is the stale-id case.
        try:
            subscriber.acknowledge(request={"subscription": unary_sub_path, "ack_ids": [ack_id]})
            check(False, "re-acking a stale ack_id should raise FAILED_PRECONDITION")
        except gexc.FailedPrecondition as exc:
            code = getattr(getattr(exc, "grpc_status_code", None), "name", "FAILED_PRECONDITION")
            print(f"  second Acknowledge (stale id) -> {code}")
            reasons = [
                getattr(d, "reason", "")
                for d in (getattr(exc, "details", None) or [])
                if hasattr(d, "reason")
            ]
            reason = reasons[0] if reasons else ""
            print(f"    ErrorInfo.reason = {reason or '(see status detail)'}")
            check(code == "FAILED_PRECONDITION", "stale unary ack returns FAILED_PRECONDITION")
            check(
                "PERMANENT_FAILURE_INVALID_ACK_ID" in str(exc),
                "carries ErrorInfo(reason=PERMANENT_FAILURE_INVALID_ACK_ID), not INVALID_ARGUMENT",
            )

        print()
        print("PASS — exactly-once: confirmed StreamingPull ack + unary stale-ack ErrorInfo.")

    finally:
        for op, fn in (
            (
                "DeleteSubscription(stream)",
                lambda: subscriber.delete_subscription(request={"subscription": stream_sub_path}),
            ),
            (
                "DeleteSubscription(unary)",
                lambda: subscriber.delete_subscription(request={"subscription": unary_sub_path}),
            ),
            ("DeleteTopic", lambda: publisher.delete_topic(request={"topic": topic_path})),
        ):
            try:
                fn()
                print(f"cleanup: {op} ok")
            except gexc.GoogleAPICallError:
                pass


if __name__ == "__main__":
    main()
