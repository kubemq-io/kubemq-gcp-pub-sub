"""KubeMQ Google Cloud Pub/Sub — subscriptions: StreamingPull (Python / google-cloud-pubsub).

The high-throughput consume path most client libraries use by default:
``CreateTopic -> CreateSubscription -> Publish(N) -> subscribe() (StreamingPull) -> Ack``.

A long-lived bidirectional ``StreamingPull`` stream is opened via the high-level
``SubscriberClient.subscribe(...)`` future with explicit ``FlowControl``; the connector pushes
messages as they arrive, the callback acks each, and the example asserts every published message
was delivered exactly once. The connector periodically closes each StreamingPull stream
(``CONNECTORS_PUB_SUB_STREAM_CLOSE_SECONDS``, default 1800 s) to force a transparent SDK reconnect.

Uses ONLY the official ``google-cloud-pubsub`` client. With ``PUBSUB_EMULATOR_HOST`` set the SDK
clears credentials, skips Google auth, and dials insecure gRPC — no code changes. Resource ids are
uuid-suffixed so concurrent runs occupy distinct ``pubsub.{t}`` / ``pubsub.sub.{s}`` channels and do
not cross-talk; both resources are deleted on exit (best-effort).
"""

from __future__ import annotations

import os
import queue
import sys
import uuid

from google.api_core.exceptions import GoogleAPICallError
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.types import FlowControl

# Number of messages published and then drained over the StreamingPull stream.
MESSAGE_COUNT = 8


def project_id() -> str:
    """Any id works — the connector parses but ignores the project segment."""
    return os.environ.get("PUBSUB_PROJECT_ID") or os.environ.get(
        "GOOGLE_CLOUD_PROJECT", "my-project"
    )


def unique(stem: str) -> str:
    """uuid-suffix a resource id so parallel runs never share a canonical channel.

    Each run then occupies distinct ``pubsub.{t}`` / ``pubsub.sub.{s}`` channels (the project
    segment is ignored, so a bare ``orders`` would collide across concurrent runs / languages).
    Ids stay within the connector rules (3..255 chars, start with a letter, charset
    ``[A-Za-z0-9._~%+-]``); topic ids must not start with ``sub.``.
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


def main() -> None:
    banner("subscriptions: StreamingPull with flow control")

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
    print(f"CreateSubscription -> {sub_path}")

    try:
        # Publish a known set of messages; remember each by its sequence attribute.
        published = set()
        for i in range(MESSAGE_COUNT):
            future = publisher.publish(topic_path, f"event-{i}".encode(), seq=str(i))
            future.result(timeout=15)
            published.add(str(i))
        print(f"Publish            -> {MESSAGE_COUNT} messages to the topic log")

        # Open a long-lived StreamingPull stream. FlowControl is the client-side ceiling on
        # outstanding (un-acked) messages — the connector also enforces its own server-side
        # MaxOutstandingMessages / MaxInflightPerSubscription safety nets.
        received: queue.Queue[str] = queue.Queue()

        def callback(message: pubsub_v1.subscriber.message.Message) -> None:
            received.put(message.attributes.get("seq", message.data.decode()))
            message.ack()

        flow_control = FlowControl(max_messages=MESSAGE_COUNT)
        streaming_future = subscriber.subscribe(
            sub_path, callback=callback, flow_control=flow_control
        )
        print("StreamingPull      -> stream open (subscribe future); draining via callback")

        drained: list[str] = []
        try:
            for _ in range(MESSAGE_COUNT):
                # Block until each message arrives over the stream and is acked in the callback.
                drained.append(received.get(timeout=30))
        finally:
            # Stop the background stream cleanly before asserting.
            streaming_future.cancel()
            try:
                streaming_future.result(timeout=10)
            except Exception:  # cancelling a live stream surfaces as an expected error
                pass

        print(f"Received           -> {len(drained)} messages over the StreamingPull stream")
        check(len(drained) == MESSAGE_COUNT, f"received all {MESSAGE_COUNT} published messages")
        check(set(drained) == published, "every published message was delivered exactly once")
        check(len(set(drained)) == len(drained), "no duplicate deliveries (each acked once)")

        print("\nStreamingPull round-trip complete — all messages drained and acked.")
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
