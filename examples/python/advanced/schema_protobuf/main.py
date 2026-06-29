"""KubeMQ Google Cloud Pub/Sub — advanced: Protobuf schema validation (Python / google-pubsub).

CreateSchema(Protobuf) -> CreateTopic(schema_settings, JSON encoding)
                       -> Publish(conforming) -> ok
                       -> Publish(non-conforming) -> batch rejected INVALID_ARGUMENT
                       -> Pull (only the conforming message is in the log).

When a topic references a schema the connector enforces it on publish: validation is part
of the atomic batch check, so the WHOLE batch is rejected (INVALID_ARGUMENT) on the first
non-conforming message and nothing is enqueued. Schemas are stored as registry records
with a server-assigned revision id and a full revision history; definitions are <= 300 KB.

Uses ONLY the official `google-cloud-pubsub` client. Resource ids are uuid-suffixed so
concurrent runs occupy distinct `pubsub.{t}` / `pubsub.sub.{s}` channels and the schema
registry records never collide. Exits 0 on success, non-zero on any failed assertion.
"""

from __future__ import annotations

import contextlib
import json
import os
import sys
import time
import uuid

from google.api_core.exceptions import InvalidArgument, NotFound
from google.cloud import pubsub_v1
from google.pubsub_v1 import SchemaServiceClient
from google.pubsub_v1.types import Encoding, Schema, SchemaSettings, Subscription, Topic

PROJECT = os.environ.get("PUBSUB_PROJECT_ID", "my-project")
SUFFIX = uuid.uuid4().hex[:8]
SCHEMA_ID = f"proto-schema-{SUFFIX}"
TOPIC_ID = f"proto-topic-{SUFFIX}"
SUB_ID = f"sub-proto-{SUFFIX}"

# A proto3 message definition the connector validates each message against.
PROTOBUF_DEFINITION = """syntax = "proto3";
message Order {
  string order_id = 1;
  double amount = 2;
}
"""


def check(condition: bool, message: str) -> None:
    """Assert an expected outcome, printing an [OK] line or exiting non-zero."""
    if condition:
        print(f"[OK] {message}")
    else:
        print(f"[FAIL] {message}")
        raise SystemExit(1)


def drain(
    subscriber: pubsub_v1.SubscriberClient, sub_path: str, deadline: float = 5.0
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
    print(f"== advanced/schema_protobuf — schema '{SCHEMA_ID}', topic '{TOPIC_ID}' ==")
    schemas = SchemaServiceClient()
    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()
    schema_path = schemas.schema_path(PROJECT, SCHEMA_ID)
    topic_path = publisher.topic_path(PROJECT, TOPIC_ID)
    sub_path = subscriber.subscription_path(PROJECT, SUB_ID)

    try:
        created = schemas.create_schema(
            request={
                "parent": f"projects/{PROJECT}",
                "schema": Schema(type_=Schema.Type.PROTOCOL_BUFFER, definition=PROTOBUF_DEFINITION),
                "schema_id": SCHEMA_ID,
            }
        )
        print(f"CreateSchema(Protobuf) -> {created.name}  revision={created.revision_id}")
        check(bool(created.revision_id), "schema created with a server-assigned revision id")

        publisher.create_topic(
            request=Topic(
                name=topic_path,
                schema_settings=SchemaSettings(schema=schema_path, encoding=Encoding.JSON),
            )
        )
        print(f"CreateTopic -> {topic_path}  (schema_settings -> {SCHEMA_ID}, encoding=JSON)")
        subscriber.create_subscription(request=Subscription(name=sub_path, topic=topic_path))
        print(f"CreateSubscription -> {sub_path}  (Queue pubsub.sub.{SUB_ID})")

        # Conforming publish: matches the proto3 message (string order_id, double amount).
        conforming = json.dumps({"order_id": "P-1", "amount": 99.0}).encode()
        message_id = publisher.publish(topic_path, conforming).result(timeout=10)
        print(f"Publish(conforming) -> message_id={message_id}")
        check(bool(message_id), "conforming message accepted (message-id returned)")

        # Non-conforming publish: amount is a string, not a double -> rejected.
        non_conforming = json.dumps({"order_id": "P-2", "amount": "not-a-number"}).encode()
        rejected = False
        try:
            publisher.publish(topic_path, non_conforming).result(timeout=10)
        except InvalidArgument:
            rejected = True
        print("Publish(non-conforming) -> schema-invalid msg → batch rejected INVALID_ARGUMENT")
        check(rejected, "non-conforming message rejected with INVALID_ARGUMENT")

        # Only the single conforming message reached the topic log.
        delivered = drain(subscriber, sub_path)
        print(f"Pull -> {delivered}")
        check(
            delivered == ['{"order_id": "P-1", "amount": 99.0}'],
            "only the conforming message was enqueued (rejected batch enqueues nothing)",
        )

        print(
            "\nPASS: Protobuf schema enforced on publish — conforming ok, non-conforming rejected."
        )
    finally:
        with contextlib.suppress(NotFound, Exception):
            subscriber.delete_subscription(subscription=sub_path)
        with contextlib.suppress(NotFound, Exception):
            publisher.delete_topic(topic=topic_path)
        with contextlib.suppress(NotFound, Exception):
            schemas.delete_schema(name=schema_path)
        sys.stdout.flush()


if __name__ == "__main__":
    main()
