"""Simple Kafka tutorial implementation using SQLite for persistence."""
import json
import os
import queue
import sqlite3
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

DB_PATH = Path(os.environ.get("SIMPLE_KAFKA_DB", "simple_kafka.db")).resolve()
DEFAULT_TOPIC = os.environ.get("KAFKA_TOPIC", "car_support_tickets")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class SimpleKafkaBroker:
    """Kafka-like message broker backed by SQLite."""

    def __init__(self, db_path: Optional[Path] = None):
        self.db_path = Path(db_path or DB_PATH)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.running = True
        self._lock = threading.Lock()
        self._consumer_threads: Dict[str, threading.Thread] = {}
        self._consumer_queues: Dict[str, queue.Queue] = {}
        self._init_db()

    def _init_db(self) -> None:
        conn = self._connect()
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS topics (
                    name TEXT PRIMARY KEY
                );
                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    topic TEXT NOT NULL,
                    message TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    claimed_by TEXT,
                    delivered_at TEXT
                );
                CREATE TABLE IF NOT EXISTS consumer_offsets (
                    consumer_id TEXT NOT NULL,
                    topic TEXT NOT NULL,
                    last_message_id INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (consumer_id, topic)
                );
                """
            )
            # Ensure new columns exist if the database was created with an older schema
            try:
                conn.execute("ALTER TABLE messages ADD COLUMN claimed_by TEXT")
            except sqlite3.OperationalError:
                pass
            try:
                conn.execute("ALTER TABLE messages ADD COLUMN delivered_at TEXT")
            except sqlite3.OperationalError:
                pass
            conn.commit()
        finally:
            conn.close()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    @contextmanager
    def _conn(self) -> Any:
        conn = self._connect()
        try:
            yield conn
        finally:
            conn.close()

    def create_topic(self, topic_name: str) -> None:
        topic_name = topic_name.strip()
        if not topic_name:
            print("[broker] Cannot create topic with empty name")
            return
        with self._conn() as conn:
            with conn:
                conn.execute("INSERT OR IGNORE INTO topics(name) VALUES (?)", (topic_name,))
        print(f"[broker] Topic ready: {topic_name}")

    def publish_message(self, topic_name: str, message: str) -> bool:
        if not topic_name:
            print("[broker] Topic name is required")
            return False
        self.create_topic(topic_name)
        timestamp = _now_iso()
        with self._conn() as conn:
            try:
                with conn:
                    conn.execute(
                        "INSERT INTO messages(topic, message, created_at, claimed_by, delivered_at) VALUES (?, ?, ?, NULL, NULL)",
                        (topic_name, message, timestamp),
                    )
                print(f"[producer] Published to {topic_name}: {message[:60]}")
                return True
            except Exception as exc:
                print(f"[broker] Failed to publish message: {exc}")
                return False

    def subscribe_to_topic(self, topic_name: str, consumer_id: str) -> queue.Queue:
        self.create_topic(topic_name)
        with self._conn() as conn:
            with conn:
                conn.execute(
                    "INSERT OR IGNORE INTO consumer_offsets(consumer_id, topic, last_message_id) VALUES (?, ?, 0)",
                    (consumer_id, topic_name),
                )
        consumer_queue: queue.Queue = queue.Queue()
        key = f"{topic_name}:{consumer_id}"
        with self._lock:
            if key in self._consumer_threads:
                return self._consumer_queues[key]
            worker = threading.Thread(
                target=self._consumer_worker,
                args=(topic_name, consumer_id, consumer_queue),
                daemon=True,
            )
            self._consumer_threads[key] = worker
            self._consumer_queues[key] = consumer_queue
            worker.start()
        print(f"[consumer] {consumer_id} subscribed to {topic_name}")
        return consumer_queue

    def _consumer_worker(self, topic_name: str, consumer_id: str, consumer_queue: queue.Queue) -> None:
        while self.running:
            message = self._fetch_next_message(topic_name, consumer_id)
            if message is None:
                time.sleep(0.5)
                continue
            consumer_queue.put(message)

    def _fetch_next_message(self, topic_name: str, consumer_id: str) -> Optional[Dict[str, Any]]:
        with self._conn() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                message_row = conn.execute(
                    "SELECT id, message FROM messages WHERE topic = ? AND claimed_by IS NULL ORDER BY id ASC LIMIT 1",
                    (topic_name,),
                ).fetchone()
                if not message_row:
                    conn.execute("COMMIT")
                    return None
                delivered_at = _now_iso()
                conn.execute(
                    "UPDATE messages SET claimed_by = ?, delivered_at = ? WHERE id = ?",
                    (consumer_id, delivered_at, message_row["id"]),
                )
                conn.execute(
                    "UPDATE consumer_offsets SET last_message_id = ? WHERE consumer_id = ? AND topic = ?",
                    (message_row["id"], consumer_id, topic_name),
                )
                conn.commit()
                return {
                    "id": str(message_row["id"]),
                    "timestamp": delivered_at,
                    "data": message_row["message"],
                    "topic": topic_name,
                }
            except sqlite3.Error as exc:
                conn.rollback()
                print(f"[broker] Consumer error: {exc}")
                time.sleep(0.5)
                return None

    def get_topic_stats(self, topic_name: str) -> Dict[str, Any]:
        with self._conn() as conn:
            total_messages = conn.execute(
                "SELECT COUNT(*) FROM messages WHERE topic = ?",
                (topic_name,),
            ).fetchone()[0]
            pending = conn.execute(
                "SELECT COUNT(*) FROM messages WHERE topic = ? AND claimed_by IS NULL",
                (topic_name,),
            ).fetchone()[0]
            consumer_count = conn.execute(
                "SELECT COUNT(*) FROM consumer_offsets WHERE topic = ?",
                (topic_name,),
            ).fetchone()[0]
        return {
            "topic": topic_name,
            "queue_size": pending,
            "consumer_count": consumer_count,
            "total_messages": total_messages,
        }

    def list_topics(self) -> List[str]:
        with self._conn() as conn:
            rows = conn.execute("SELECT name FROM topics ORDER BY name").fetchall()
        return [row[0] for row in rows]

    def stop(self) -> None:
        self.running = False
        for key, worker in list(self._consumer_threads.items()):
            worker.join(timeout=1.0)
            print(f"[broker] Stopped worker {key}")
        print("[broker] Shutdown complete")


broker = SimpleKafkaBroker()


class SimpleKafkaProducer:
    def __init__(self, broker_instance: SimpleKafkaBroker):
        self.broker = broker_instance
        self.message_count = 0

    def publish(self, topic_name: str, message: str) -> bool:
        success = self.broker.publish_message(topic_name, message)
        if success:
            self.message_count += 1
        return success

    def flush(self) -> None:
        pass


class SimpleKafkaConsumer:
    def __init__(self, broker_instance: SimpleKafkaBroker, consumer_id: Optional[str] = None):
        self.broker = broker_instance
        self.consumer_id = consumer_id or f"consumer_{int(time.time())}"
        self.consumer_queue: Optional[queue.Queue] = None
        self.message_count = 0

    def subscribe(self, topic_name: str) -> None:
        self.consumer_queue = self.broker.subscribe_to_topic(topic_name, self.consumer_id)
        print(f"[consumer] Listening to {topic_name}")

    def poll(self, timeout: float = 1.0) -> Optional[Dict[str, Any]]:
        if not self.consumer_queue:
            return None
        try:
            message = self.consumer_queue.get(timeout=timeout)
            self.message_count += 1
            return message
        except queue.Empty:
            return None

    def close(self) -> None:
        print(f"[consumer] {self.consumer_id} closed")


def main() -> None:
    print("Simple Kafka Setup (No Docker Required)")
    print("=" * 60)
    topic_name = DEFAULT_TOPIC
    broker.create_topic(topic_name)

    producer = SimpleKafkaProducer(broker)
    consumer = SimpleKafkaConsumer(broker)
    consumer.subscribe(topic_name)

    test_messages = [
        json.dumps({"txid": "test1", "car_model": "Peugeot 208", "name": "Alice"}),
        json.dumps({"txid": "test2", "car_model": "Citroen C3", "name": "Bob"}),
        json.dumps({"txid": "test3", "car_model": "Renault Megane", "name": "Charlie"}),
    ]

    print("\nPublishing test messages...")
    for message in test_messages:
        producer.publish(topic_name, message)
        time.sleep(0.2)

    print("\nConsuming messages...")
    for index in range(5):
        message = consumer.poll(timeout=2.0)
        if message:
            print(f"  Message {index + 1}: {message['data']}")
        else:
            print("  No message received (timeout)")

    stats = broker.get_topic_stats(topic_name)
    print("\nTopic statistics:")
    print(f"  Topic: {stats['topic']}")
    print(f"  Queue size: {stats['queue_size']}")
    print(f"  Consumer count: {stats['consumer_count']}")
    print(f"  Total messages: {stats['total_messages']}")
    print(f"  Messages published: {producer.message_count}")
    print(f"  Messages consumed: {consumer.message_count}")

    consumer.close()
    broker.stop()
    print("\nSimple Kafka test completed!")


if __name__ == "__main__":
    main()


