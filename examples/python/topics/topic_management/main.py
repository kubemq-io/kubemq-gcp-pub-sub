"""KubeMQ Google Cloud Pub/Sub — topics: topic management (Python / google-cloud-pubsub).

CreateTopic -> GetTopic -> UpdateTopic(labels) -> ListTopics -> ListTopicSubscriptions
-> DeleteTopic (tombstone) -> assert sub.-prefixed topic id is rejected.

Exercises the Publisher admin surface and asserts: the label round-trip via
``UpdateTopic`` + ``FieldMask``; that a freshly created topic appears in
``ListTopics``; that ``ListTopicSubscriptions`` reflects an attached subscription;
that ``DeleteTopic`` tombstones the topic (a later ``GetTopic`` -> ``NOT_FOUND``);
and that a topic id starting with the reserved ``sub.`` prefix is rejected
(``INVALID_ARGUMENT`` — gotcha #7).

Uses ONLY the official ``google-cloud-pubsub`` client driven via
``PUBSUB_EMULATOR_HOST`` (zero code changes). Resource names are uuid-suffixed
(channel-isolation rule) and deleted on exit (best-effort).
"""

from __future__ import annotations

import os
import sys
import uuid

from google.api_core.exceptions import (
    GoogleAPICallError,
    InvalidArgument,
    NotFound,
)
from google.cloud import pubsub_v1
from google.protobuf.field_mask_pb2 import FieldMask


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
    topic_id = rname("managed")
    sub_id = rname("sub-managed")
    banner(f"topics/topic_management — topic '{topic_id}'")

    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()
    topic_path = publisher.topic_path(proj, topic_id)
    sub_path = subscriber.subscription_path(proj, sub_id)

    try:
        # 1) CreateTopic.
        created = publisher.create_topic(request={"name": topic_path})
        print(f"CreateTopic -> {created.name}")

        # 2) GetTopic round-trips the path.
        got = publisher.get_topic(request={"topic": topic_path})
        print(f"GetTopic -> {got.name}")
        check(got.name == topic_path, "GetTopic returns the created topic path")

        # 3) UpdateTopic: set labels via a FieldMask (only the masked field changes).
        got.labels["team"] = "platform"
        got.labels["env"] = "dev"
        updated = publisher.update_topic(
            request={"topic": got, "update_mask": FieldMask(paths=["labels"])}
        )
        print(f"UpdateTopic labels -> {dict(updated.labels)}")
        check(
            dict(updated.labels) == {"team": "platform", "env": "dev"},
            "labels round-trip through UpdateTopic",
        )

        # 4) ListTopics: the new topic is present.
        topics = [t.name for t in publisher.list_topics(request={"project": f"projects/{proj}"})]
        print(f"ListTopics -> {len(topics)} topic(s) in project")
        check(topic_path in topics, "created topic appears in ListTopics")

        # 5) Attach a subscription, then ListTopicSubscriptions reflects it.
        subscriber.create_subscription(request={"name": sub_path, "topic": topic_path})
        attached = list(publisher.list_topic_subscriptions(request={"topic": topic_path}))
        print(f"ListTopicSubscriptions -> {attached}")
        check(sub_path in attached, "attached subscription appears in ListTopicSubscriptions")

        # 6) DeleteTopic tombstones the topic: a later GetTopic -> NOT_FOUND.
        publisher.delete_topic(request={"topic": topic_path})
        print("DeleteTopic -> tombstoned")
        try:
            publisher.get_topic(request={"topic": topic_path})
            check(False, "GetTopic after DeleteTopic should raise NotFound")
        except NotFound:
            check(True, "GetTopic after DeleteTopic -> NOT_FOUND (tombstone)")

        # 7) Gotcha #7: a topic id starting with the reserved 'sub.' prefix is rejected.
        bad_path = publisher.topic_path(proj, rname("sub.illegal"))
        try:
            publisher.create_topic(request={"name": bad_path})
            check(False, "a 'sub.'-prefixed topic id must be rejected")
        except InvalidArgument:
            check(True, "topic id starting with 'sub.' rejected -> INVALID_ARGUMENT (gotcha #7)")

        print("\nPASS — topic management surface exercised.")
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
