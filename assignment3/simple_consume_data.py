"""Simple consumer that prints messages from the local broker."""
import json
import os
import signal
import sys
from typing import Any, Dict

from simple_kafka_setup import DEFAULT_TOPIC, SimpleKafkaConsumer, broker

HEADER = "Simple Kafka Consumer for EV Support Tickets"
MAX_MESSAGES = int(os.environ.get("KAFKA_MAX_MESSAGES", "0"))

terminated = False


def handle_signal(signum, frame):  # type: ignore[arg-type]
    global terminated
    terminated = True
    print("\nStopping consumer...")


signal.signal(signal.SIGINT, handle_signal)


def render_message(message: Dict[str, Any]) -> str:
    try:
        payload = json.loads(message["data"])
    except json.JSONDecodeError:
        payload = {"raw": message["data"]}
    lines = [
        f"Message ID: {message['id']}",
        f"Published: {message['timestamp']}",
    ]
    if isinstance(payload, dict):
        for key in ["txid", "name", "email", "car_model", "item", "product", "purchase_time", "address"]:
            if key in payload and payload[key]:
                lines.append(f"{key}: {payload[key]}")
    else:
        lines.append(f"Payload: {payload}")
    return "\n  ".join(lines)


def main() -> None:
    global terminated
    topic = os.environ.get("KAFKA_TOPIC", DEFAULT_TOPIC)
    consumer_id = os.environ.get("KAFKA_CONSUMER_GROUP")
    consumer = SimpleKafkaConsumer(broker, consumer_id=consumer_id)
    print(HEADER)
    print("-" * len(HEADER))
    print(f"Topic: {topic}")
    print("Waiting for messages... (Ctrl+C to stop)")

    consumer.subscribe(topic)
    seen = 0

    while not terminated:
        message = consumer.poll(timeout=1.0)
        if not message:
            continue
        print("\nNew message:")
        print(f"  {render_message(message)}")
        sys.stdout.flush()
        seen += 1
        if MAX_MESSAGES and seen >= MAX_MESSAGES:
            terminated = True

    consumer.close()
    broker.stop()


if __name__ == "__main__":
    main()
