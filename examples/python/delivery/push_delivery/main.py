"""KubeMQ Google Cloud Pub/Sub — delivery: push delivery (Python / google-cloud-pubsub).

Start a local HTTP endpoint -> CreateTopic -> CreateSubscription with a
push_config pointing at the endpoint -> Publish one message -> the connector's
per-subscription push worker POSTs the wrapped JSON envelope to the endpoint,
which returns 2xx (ack). Asserts the envelope shape: base64 `data`, the user
attributes, `messageId` / `publishTime`, and the enclosing `subscription`.

This is an unmodified `google-cloud-pubsub` program for the admin side. Setting
PUBSUB_EMULATOR_HOST makes the client clear credentials, skip Google auth, and
dial insecure gRPC at the KubeMQ Pub/Sub connector (default localhost:8085) —
zero code changes. The endpoint is the receiving HTTP server (the push target).

Connector behaviour (spec S3.5 / docs/guides/push-delivery.md): a subscription
with a push_config is served by a per-subscription delivery worker that pulls
from the sub queue pubsub.sub.{s} and POSTs a wrapped envelope
`{message:{data(base64),attributes,messageId,publishTime,orderingKey}, subscription}`.
2xx -> ack; non-2xx/timeout -> retry with backoff. Endpoints must be HTTPS —
plain HTTP is allowed only for localhost (this example).
"""

from __future__ import annotations

import base64
import json
import os
import socket
import sys
import threading
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer

from google.api_core.exceptions import GoogleAPICallError
from google.cloud import pubsub_v1
from google.pubsub_v1.types import PushConfig

PROJECT = os.environ.get("PUBSUB_PROJECT_ID", os.environ.get("GOOGLE_CLOUD_PROJECT", "my-project"))

# Unique-per-run resource names (channel-isolation rule, spec S9).
RUN = uuid.uuid4().hex[:8]
TOPIC_ID = f"events-{RUN}"
SUB_ID = f"sub-push-{RUN}"

BODY = "push-me"
ATTRS = {"type": "order", "region": "eu"}

# A thread-safe slot for the single delivered envelope + an event to wake the
# main thread once the endpoint has been POSTed.
_received: dict[str, object] = {}
_got = threading.Event()


def check(condition: bool, message: str) -> None:
    if condition:
        print(f"  [OK] {message}")
    else:
        print(f"  [FAIL] {message}", file=sys.stderr)
        raise SystemExit(1)


class PushHandler(BaseHTTPRequestHandler):
    """Receives the connector's push POST and returns 2xx so the message is acked."""

    def do_POST(self) -> None:  # noqa: N802 (http.server API)
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length)
        try:
            _received["envelope"] = json.loads(raw)
        except json.JSONDecodeError:
            _received["envelope"] = {"_raw": raw.decode("utf-8", "replace")}
        self.send_response(204)  # 2xx -> ack
        self.end_headers()
        _got.set()

    def log_message(self, *_args) -> None:  # silence default request logging
        pass


def free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def main() -> None:
    port = free_port()
    endpoint = f"http://localhost:{port}/push"
    print(f"=== Pub/Sub push delivery — topic '{TOPIC_ID}' / sub '{SUB_ID}' ===")
    print(f"  emulator : {os.environ.get('PUBSUB_EMULATOR_HOST', '(unset!)')}")
    print(f"  project  : {PROJECT} (parsed but ignored by the connector)")
    print(f"  endpoint : {endpoint}  (plain HTTP allowed for localhost)")
    print()

    # Start the local push receiver.
    httpd = HTTPServer(("127.0.0.1", port), PushHandler)
    server_thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    server_thread.start()
    print(f"Local push endpoint listening on {endpoint}")

    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()
    topic_path = publisher.topic_path(PROJECT, TOPIC_ID)
    sub_path = subscriber.subscription_path(PROJECT, SUB_ID)

    try:
        publisher.create_topic(request={"name": topic_path})
        print(f"CreateTopic            -> {topic_path}")

        subscriber.create_subscription(
            request={
                "name": sub_path,
                "topic": topic_path,
                "push_config": PushConfig(push_endpoint=endpoint),
            }
        )
        print(f"CreateSubscription     -> {sub_path}  (push_config -> {endpoint})")
        print()

        msg_id = publisher.publish(topic_path, BODY.encode("utf-8"), **ATTRS).result(timeout=30)
        print(f"Publish -> body={BODY!r} attrs={ATTRS} id={msg_id}")
        print()

        print("Waiting for the connector push worker to POST the wrapped envelope...")
        check(_got.wait(timeout=30), "endpoint received a push POST within 30s")

        envelope = _received.get("envelope")
        print("Received envelope:")
        print(json.dumps(envelope, indent=2))
        print()

        check(
            isinstance(envelope, dict) and "message" in envelope,
            "envelope is wrapped (has 'message')",
        )
        message = envelope["message"]
        check("subscription" in envelope, "envelope carries the 'subscription' field")
        decoded = base64.b64decode(message["data"]).decode("utf-8")
        check(decoded == BODY, f"message.data base64-decodes to {BODY!r}")
        check(
            message.get("attributes", {}).get("type") == "order"
            and message.get("attributes", {}).get("region") == "eu",
            "message.attributes carries the user attributes",
        )
        check(bool(message.get("messageId")), "message.messageId is present")
        check(bool(message.get("publishTime")), "message.publishTime is present")

        print()
        print("PASS — push delivery: wrapped envelope POSTed to the localhost endpoint, 2xx acked.")

    finally:
        httpd.shutdown()
        for op, fn in (
            (
                "DeleteSubscription",
                lambda: subscriber.delete_subscription(request={"subscription": sub_path}),
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
