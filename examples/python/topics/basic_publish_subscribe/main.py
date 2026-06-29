"""KubeMQ Google Cloud Pub/Sub — topics: basic publish & subscribe (Python / google-cloud-pubsub).

The full Pub/Sub round-trip against the KubeMQ Pub/Sub connector:
CreateTopic -> CreateSubscription -> Publish -> Pull -> Acknowledge.

Uses ONLY the official ``google-cloud-pubsub`` client. The client honours the
standard ``PUBSUB_EMULATOR_HOST`` env var: it clears credentials, skips Google
auth, and dials insecure gRPC — exactly as against Google's local emulator, with
zero code changes. The connector lives inside kubemq-server (default gRPC port
8085). See ../../README.md and ../../../SHARED-CONVENTIONS.md.

Resource names are uuid-suffixed (channel-isolation rule) so concurrent runs use
distinct ``pubsub.{t}`` / ``pubsub.sub.{s}`` channels and never cross-talk; the
topic/subscription are deleted on exit (best-effort).
"""

from __future__ import annotations

import os
import sys
import uuid

from google.api_core.exceptions import GoogleAPICallError, NotFound
from google.cloud import pubsub_v1


def project_id() -> str:
    """Any id works — the connector parses but ignores the project segment."""
    return os.environ.get("PUBSUB_PROJECT_ID") or os.environ.get(
        "GOOGLE_CLOUD_PROJECT", "my-project"
    )


def rname(base: str) -> str:
    """uuid-suffix a resource id so parallel runs occupy private channels.

    Topic id 'orders' -> 'orders-<uuid8>'; the connector maps it to the
    Events Store log 'pubsub.orders-<uuid8>'. Ids stay within the connector
    rules (3..255 chars, start with a letter, charset [A-Za-z0-9._~%+-]).
    """
    return f"{base}-{uuid.uuid4().hex[:8]}"


def banner(title: str) -> None:
    print(f"=== {title} ===")
    print(f"  emulator : {os.environ.get('PUBSUB_EMULATOR_HOST', '(unset!)')}")
    print(f"  project  : {project_id()} (parsed but ignored by the connector)")
    print("  auth     : none / insecure gRPC (emulator drop-in)")
    print()


def check(condition: bool, message: str) -> None:
    """Assert an expected outcome; exit non-zero on failure.

    Examples are runnable PROOFS, not demos — a missing message or a wrong body
    must fail the process.
    """
    if condition:
        print(f"  [OK] {message}")
    else:
        print(f"  [FAIL] {message}", file=sys.stderr)
        raise SystemExit(1)


def main() -> None:
    proj = project_id()
    topic_id = rname("orders")
    sub_id = rname("sub-orders")
    banner(f"topics/basic_publish_subscribe — topic '{topic_id}'")

    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()
    topic_path = publisher.topic_path(proj, topic_id)
    sub_path = subscriber.subscription_path(proj, sub_id)

    try:
        # 1) CreateTopic -> Events Store log pubsub.{topic_id}
        created = publisher.create_topic(request={"name": topic_path})
        print(f"CreateTopic -> {created.name}")
        check(created.name == topic_path, "created topic name matches the requested path")

        # 2) CreateSubscription -> Queue pubsub.sub.{sub_id} (default ack deadline 10s)
        sub = subscriber.create_subscription(request={"name": sub_path, "topic": topic_path})
        print(f"CreateSubscription -> {sub.name}  (ack_deadline={sub.ack_deadline_seconds}s)")
        check(sub.ack_deadline_seconds == 10, "default ack deadline is the connector floor (10s)")

        # 3) Publish one message; the publish future resolves to the server id.
        body = b"hello from google-cloud-pubsub"
        future = publisher.publish(topic_path, body, source="basic_publish_subscribe")
        message_id = future.result(timeout=15)
        print(f"Publish -> message_id={message_id}")
        check(bool(message_id), "publish returned a server-assigned message id")

        # 4) Pull and read the message back from the per-subscription queue.
        resp = subscriber.pull(request={"subscription": sub_path, "max_messages": 10}, timeout=20)
        received = list(resp.received_messages)
        check(len(received) == 1, "pulled exactly 1 message")
        msg = received[0].message
        print(f"Pull -> data={msg.data.decode()!r}  message_id={msg.message_id}")
        print(
            f"        publish_time={msg.publish_time.isoformat()}  "
            f"attributes={dict(msg.attributes)}"
        )
        check(msg.data == body, "pulled body equals the published body")
        check(msg.message_id == message_id, "pulled message_id matches the publish id")
        # Reserved tags (_pubsub_*) are stripped for Pub/Sub clients; only the
        # caller's own attribute survives.
        check(
            dict(msg.attributes) == {"source": "basic_publish_subscribe"},
            "attributes round-trip; reserved _pubsub_* tags are not exposed",
        )

        # 5) Acknowledge by ack_id; the message leaves the subscription queue.
        ack_ids = [r.ack_id for r in received]
        subscriber.acknowledge(request={"subscription": sub_path, "ack_ids": ack_ids})
        print(f"Acknowledge -> {len(ack_ids)} ack_id(s)")

        drain = subscriber.pull(request={"subscription": sub_path, "max_messages": 10}, timeout=5)
        check(len(drain.received_messages) == 0, "subscription empty after ack (no redelivery)")

        print("\nPASS — basic publish/subscribe round-trip complete.")
    finally:
        # Best-effort cleanup: delete subscription then topic so re-runs start clean.
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
        pass  # already gone / connector down — cleanup is best-effort.


if __name__ == "__main__":
    main()
