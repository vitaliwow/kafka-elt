import json
import logging
import os
import random
import time
import typing as t
from dataclasses import dataclass
from functools import cached_property

import pandas as pd
from confluent_kafka import Producer

from topics import TOPICS

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class KafkaPublisher:
    bootstrap_servers: list[str]
    client_id: str
    acks: t.Literal["all"] = "all"
    retries: int = 3
    compression_type: t.Literal["gzip"] = "gzip"


    def delivery_report(self, err, msg):
        """Called once for each message to indicate delivery result"""
        if err is not None:
            logger.error(f'❌ Message delivery failed: {err}')
        else:
            logger.info(
                f"✅ Message delivered to {msg.topic()} "
                f"[partition {msg.partition()}] at offset {msg.offset()}"
            )

    def publish_message(
        self,
        topic: str,
        message: dict,
        key: str | None = None,
        partition: int | None = None,
    ) -> bool:
        """Publish a message to the specified topic"""
        try:
            # Convert message to JSON string if it's a dictionary
            if isinstance(message, dict):
                message = json.dumps(message)

            # Trigger any available delivery report callbacks from previous produce() calls
            self.producer.poll(0)

            # Asynchronously produce a message
            payload = dict(
                topic=topic,
                value=message.encode('utf-8'),
                key=str(key).encode('utf-8') if key else None,
                callback=self.delivery_report
            )
            if partition is not None:
                payload["partition"] = partition
            self.producer.produce(
                topic=topic,
                value=message.encode('utf-8'),
                key=str(key).encode('utf-8') if key else None,
                callback=self.delivery_report
            )

            return True

        except Exception as e:
            logger.error(f"❌ Message delivery failed: {e}")
            return False

    def flush(self):
        """Wait for all outstanding messages to be delivered"""
        self.producer.flush()

    def publish_batch(self, topic, messages):
        """Publish multiple messages"""
        successes = 0
        failures = 0

        for message in messages:
            success = self.publish_message(topic, message)
            if success:
                successes += 1
            else:
                failures += 1

        # Wait for all messages to be sent
        self.flush()
        logger.info(f"📦 Batch completed: {successes} successful, {failures} failed")
        return successes, failures

    @property
    def _config(self) -> dict[str, t.Any]:
        return {
            "bootstrap.servers": ",".join(self.bootstrap_servers),
            "client.id": self.client_id,
            "acks": self.acks,
            "retries": self.retries,
            "compression.type": self.compression_type,
        }

    @cached_property
    def producer(self) -> Producer:
        producer = Producer(self._config)
        logger.info(f"✅ Producer connected to Kafka at {self.bootstrap_servers}")

        return producer


def stream_source_csv(
    bootstrap_servers: list[str],
    topics: list[str],
    chunk_size: int = 100000,
) -> None:
    try:
        publisher = KafkaPublisher(
            bootstrap_servers=bootstrap_servers,
            client_id="producer",
        )

        logger.info("🚀 Starting producer...")



        for topic in topics:
            logger.info(f"🎯 Publishing to topic: {topic}")
            for chunk_idx, chunk in enumerate(
                    pd.read_csv(
                        f"dataset/{topic}.csv",
                        chunksize=chunk_size,
                    )
            ):
                logger.info(f"Processing chunk {chunk_idx + 1} of file {topic}")

                for row_idx, row in chunk.iterrows():
                    message = {
                        'timestamp': int(time.time() * 1000),
                        'data': row.replace({pd.NaT: None}).to_dict(),
                    }
                    publisher.publish_message(
                        topic=topic,
                        message=message,
                        key=os.path.basename(topic),
                        partition=random.randint(0, 2)
                    )

                    logger.info(f"Successfully published to topic: {topic}")

    except KeyboardInterrupt:
        logger.info("⏹️ Producer interrupted by user")
    except Exception as e:
        logger.error(f"💥 Producer failed: {e}")
    else:
        publisher.flush()


if __name__ == "__main__":

    stream_source_csv(
        bootstrap_servers=["localhost:29092", ],
        topics=TOPICS,
    )
