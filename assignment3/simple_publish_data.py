"""Simple publisher that reads JSON lines from stdin and stores them in the local broker."""
import json
import os
import sys
from typing import Iterable

from simple_kafka_setup import DEFAULT_TOPIC, SimpleKafkaProducer, broker

HEADER = "Simple Kafka Publisher for EV Support Tickets"


def iter_messages(source: Iterable[str]) -> Iterable[str]:
    for raw in source:
        line = raw.strip()
        if not line:
            continue
        yield line


def main() -> None:
    topic = os.environ.get("KAFKA_TOPIC", DEFAULT_TOPIC)
    producer = SimpleKafkaProducer(broker)
    print(HEADER)
    print("-" * len(HEADER))
    print(f"Topic: {topic}")
    print("Publishing messages... (Ctrl+C to stop)")

    published = 0
    failed = 0
    try:
        for record in iter_messages(sys.stdin):
            try:
                json.loads(record)
            except json.JSONDecodeError:
                print(f"[warn] Skipping invalid JSON: {record[:60]}")
                failed += 1
                continue
            if producer.publish(topic, record):
                published += 1
            else:
                failed += 1
    except KeyboardInterrupt:
        print("\nInterrupted by user")

    total = published + failed if (published + failed) else 1
    success_rate = (published / total) * 100
    print("Publishing complete!")
    print(f"  Messages published: {published}")
    print(f"  Messages failed: {failed}")
    print(f"  Success rate: {success_rate:.1f}%")

    stats = broker.get_topic_stats(topic)
    print("\nTopic statistics:")
    print(f"  Queue size: {stats['queue_size']}")
    print(f"  Consumer count: {stats['consumer_count']}")
    print(f"  Total messages: {stats['total_messages']}")


if __name__ == "__main__":
    main()
