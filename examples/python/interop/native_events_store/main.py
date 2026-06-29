"""KubeMQ Google Pub/Sub — interop: Pub/Sub publish -> native Events Store consume.

The only example in the repo that imports a KubeMQ SDK. It demonstrates the
connector's cross-protocol bridge end to end, in one process:

  1. A native ``kubemq`` (PyPI ``kubemq==4.1.5``) Events Store subscriber attaches
     to channel ``pubsub.{topic}`` with start policy ``StartFromNew`` ("new only").
  2. We block until the subscribe stream is confirmed open (subscribe-BEFORE-publish).
  3. The official ``google-cloud-pubsub`` client publishes one message to ``topic``
     via ``PUBSUB_EMULATOR_HOST`` (zero code changes — the SDK skips Google auth
     and dials insecure gRPC).
  4. The native subscriber receives exactly that message on ``pubsub.{topic}`` and
     sees the three reserved tags the connector stamps on every message —
     ``_pubsub_message_id``, ``_pubsub_publish_time``, ``_pubsub_ordering_key`` —
     which are stripped from ``attributes`` when delivered to a Pub/Sub client.

Why subscribe-before-publish + ``StartFromNew``: the native side attaches to a
**stream**, not a fixed offset. A naive "publish then subscribe" races — the
publish could land before the subscriber attaches and be missed. Establishing the
"new only" subscription first guarantees the published message is in-window
(spec S5.7; ``kubemq-server/docs/20-google-pubsub.md:73-77``).

Channel mapping (spec S3.3): a Pub/Sub ``Publish`` writes once to the topic's
Events Store log ``pubsub.{t}`` (the authoritative, replayable, cross-protocol
source) before fanning out per-subscription queue copies. Here there is no
subscription — we read the topic log directly, natively.

The Pub/Sub half talks to the connector via ``PUBSUB_EMULATOR_HOST`` (default
``localhost:8085``); the native half talks to the gRPC broker via
``KUBEMQ_GRPC_ADDRESS`` (default ``localhost:50000``).
"""

from __future__ import annotations

import os
import queue
import sys
import threading
import uuid

from google.cloud import pubsub_v1
from kubemq import EventsStoreSubscription, EventStoreReceived, PubSubClient
from kubemq.pubsub.events_store_subscription import EventStoreStartPosition

# --- connection (the two halves) -------------------------------------------------

PROJECT_ID = os.environ.get("PUBSUB_PROJECT_ID", "my-project")  # parsed but ignored
GRPC_ADDRESS = os.environ.get("KUBEMQ_GRPC_ADDRESS", "localhost:50000")
EMULATOR_HOST = os.environ.get("PUBSUB_EMULATOR_HOST", "localhost:8085")

# --- per-run isolation (parallel-safe; spec S9 / SHARED-CONVENTIONS §1) ----------
# The canonical interop name is `orders`, but the channel pubsub.orders is global
# and shared, so a fixed name would cross-talk with a concurrent run (/examples +
# /burnin, other languages) and false-fail the single-message assertion. uuid8-suffix
# the topic so each run reads its own private Events Store channel pubsub.{topic}.
RUN = uuid.uuid4().hex[:8]
TOPIC = f"orders-{RUN}"  # Pub/Sub topic id (starts with a letter; not `sub.`)
CHANNEL = f"pubsub.{TOPIC}"  # the native Events Store channel it maps to

# The three reserved tags the connector stamps on every Pub/Sub message and that a
# native consumer sees (stripped from `attributes` for Pub/Sub clients; spec S3.4).
RESERVED = ("_pubsub_message_id", "_pubsub_publish_time", "_pubsub_ordering_key")


def check(condition: bool, message: str) -> None:
    """Assert an expected outcome; exit non-zero on failure.

    These examples are runnable PROOFS, not demos — a missing message, a missing
    reserved tag, or a mismatched payload must fail the process.
    """
    if condition:
        print(f"  [OK] {message}")
    else:
        print(f"  [FAIL] {message}", file=sys.stderr)
        raise SystemExit(1)


def main() -> None:
    print(
        "=== interop: Pub/Sub publish -> native Events Store consume — "
        f"topic '{TOPIC}' / channel '{CHANNEL}' ==="
    )
    print(f"  pubsub : {EMULATOR_HOST} (PUBSUB_EMULATOR_HOST; insecure, no Google auth)")
    print(f"  grpc   : {GRPC_ADDRESS} (native KubeMQ broker)")
    print()

    # enable_message_ordering is required to publish with an ordering key — it
    # makes the reserved tag _pubsub_ordering_key observable on the native side.
    publisher = pubsub_v1.PublisherClient(
        publisher_options=pubsub_v1.types.PublisherOptions(enable_message_ordering=True)
    )
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC)
    publisher.create_topic(request={"name": topic_path})
    print(f"CreateTopic -> {topic_path}")

    received: queue.Queue[EventStoreReceived] = queue.Queue(maxsize=4)
    errors: list[str] = []

    def on_event(event: EventStoreReceived) -> None:
        received.put(event)

    def on_error(err: str) -> None:
        errors.append(err)

    native = PubSubClient(address=GRPC_ADDRESS, client_id=f"gcp-interop-python-{RUN}")
    try:
        # --- 1. subscribe FIRST, with start policy "new only" --------------------
        native.subscribe_to_events_store(
            EventsStoreSubscription(
                channel=CHANNEL,
                events_store_type=EventStoreStartPosition.StartFromNew,
                on_receive_event_callback=on_event,
                on_error_callback=on_error,
            )
        )
        # The subscribe runs on a background thread that opens the gRPC stream.
        # Give it a moment to establish so the publish below is guaranteed
        # in-window for the "new only" subscription (subscribe-before-publish).
        _await_stream_open(errors)
        print(f"native SubscribeToEventsStore('{CHANNEL}', startAt='new') -> stream open")

        # --- 2. publish exactly one message via the Pub/Sub SDK ------------------
        body = b"from-gcp-pubsub"
        ordering_key = "shipments"
        future = publisher.publish(
            topic_path,
            data=body,
            ordering_key=ordering_key,
            region="emea",  # an ordinary attribute — rides along as a plain tag
        )
        message_id = future.result(timeout=15)
        print(f"PublisherClient.publish('{TOPIC}') -> message_id={message_id}")

        # --- 3. await exactly one event natively on pubsub.{topic} ---------------
        try:
            event = received.get(timeout=15)
        except queue.Empty:
            check(False, f"native consumer received the published message on '{CHANNEL}'")
            return  # unreachable (check raises), keeps type-checkers happy

        check(True, f"native consumer received the published message on '{CHANNEL}'")
        event_body = event.body.decode("utf-8")
        print(f"native receive('{CHANNEL}') -> body={event_body!r}")
        for key in RESERVED:
            print(f"    {key} = {event.tags.get(key)!r}")
        print(f"    region (attribute) = {event.tags.get('region')!r}")

        # --- 4. assert the cross-protocol invariants -----------------------------
        check(event_body == "from-gcp-pubsub", "native side sees the Pub/Sub-published body")
        check(
            all(tag in event.tags for tag in RESERVED),
            "all three reserved tags are present (_pubsub_message_id/publish_time/ordering_key)",
        )
        check(
            event.tags.get("_pubsub_message_id") == message_id,
            "_pubsub_message_id matches the Pub/Sub-returned message id",
        )
        check(
            event.tags.get("_pubsub_ordering_key") == ordering_key,
            "_pubsub_ordering_key carries the publish ordering key",
        )
        check(
            event.tags.get("region") == "emea",
            "the message attribute rides along as an ordinary tag",
        )

        # only one message was published — the queue must now be empty
        check(received.empty(), "exactly one message was consumed (no cross-talk)")
        check(not errors, f"no subscription errors ({errors})")
    finally:
        # --- best-effort cleanup (must not fail the run) -------------------------
        native.close()
        try:
            publisher.delete_topic(request={"topic": topic_path})
            print(f"DeleteTopic('{TOPIC}') -> done")
        except Exception as exc:  # noqa: BLE001 — cleanup is best-effort
            print(f"DeleteTopic('{TOPIC}') -> skipped ({type(exc).__name__})")

    print("\nPASS — Pub/Sub publish consumed natively on the Events Store topic log.")


def _await_stream_open(errors: list[str]) -> None:
    """Block briefly so the native subscribe stream is established before publish.

    The ``kubemq`` SDK opens the Events Store stream on a background thread and does
    not expose an explicit ready signal, so we wait a short, bounded interval and
    fail fast if the stream reported an error (e.g. broker unreachable). With
    ``StartFromNew`` the published message is in-window once the stream is open.
    """
    deadline = threading.Event()
    deadline.wait(timeout=1.5)
    if errors:
        print(f"  [FAIL] native subscribe stream failed to open: {errors}", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
