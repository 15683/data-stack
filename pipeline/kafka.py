import json
import os

from confluent_kafka import Producer


def producer():
    return Producer(
        {
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            "enable.idempotence": True,
            "acks": "all",
            "delivery.timeout.ms": 30000,
            "message.max.bytes": 1048576,
        }
    )


def send(client, topic: str, key: str, value: dict):
    errors = []

    def delivered(error, _message):
        if error is not None:
            errors.append(str(error))

    client.produce(
        topic,
        key=key.encode(),
        value=json.dumps(value, ensure_ascii=False).encode(),
        on_delivery=delivered,
    )
    if client.flush(35) or errors:
        raise RuntimeError(f"Kafka delivery failed: {errors}")
