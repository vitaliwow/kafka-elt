import json
import logging
import typing as t
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property

import duckdb
from confluent_kafka import Consumer, KafkaError, Message, TopicPartition

from consumer.handler import HandleOlist
from topics import TOPICS

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class KafkaConsumer:
    bootstrap_servers: list[str]
    group_id: str
    consumer_id: str
    auto_offset_reset: t.Literal["earliest", "latest"] = "earliest"
    is_running: bool = False
    message_count: int = 0

    def signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info("🛑 Received shutdown signal...")
        self.is_running = False

    def subscribe(self, topics):
        """Subscribe to one or more topics"""
        if isinstance(topics, str):
            topics = [topics]

        self.consumer.subscribe(topics)
        logger.info(f"📝 Consumer ID {self.consumer_id} Subscribed to topics: {topics}")

    def process_message(
        self,
        message: Message,
    ) -> None:
        """Process a single message - override this for custom logic"""
        try:
            # Decode the message
            value = json.loads(message.value().decode('utf-8'))
            key = message.key().decode('utf-8') if message.key() else None

            logger.info(f"📨 Consumer ID {self.consumer_id}:")
            logger.info(f"📨 Received message:")
            logger.info(f"   Key: {key}")
            logger.info(f"   Value: {value}")
            logger.info(f"   Topic: {message.topic()}")
            logger.info(f"   Partition: {message.partition()}")
            logger.info(f"   Offset: {message.offset()}")
            logger.info(f"   Timestamp: {datetime.fromtimestamp(message.timestamp()[1] / 1000)}")
            logger.info("-" * 60)

            # Example processing logic
            if value.get('type') == 'ERROR':
                logger.warning("⚠️  Error message detected - special handling needed")

            self.message_count += 1

        except Exception as e:
            logger.error(f"❌ Error processing message: {e}")

    def start_consuming(self, timeout=1.0):
        """Start consuming messages"""
        self.is_running = True
        logger.info(f"🎧 Starting to consume messages by consumer {self.consumer_id}...")

        try:
            while self.is_running:
                msg = self.consumer.poll(timeout=timeout)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        logger.info(f"Reached end of partition {msg.partition()}")
                    else:
                        logger.error(f"Consumer {self.consumer_id} error: {msg.error()}")
                else:
                    self.process_message(msg)

        except KeyboardInterrupt:
            logger.info("⏹️ Consumer interrupted by user")
        except Exception as e:
            logger.error(f"💥 Consumer {self.consumer_id} error: {e}")
        finally:
            self.close()

    def close(self):
        """Close the consumer connection"""
        self.consumer.close()
        logger.info(f"🔴 Consumer {self.consumer_id} closed. Total messages processed: {self.message_count}")

    @property
    def config(self) -> dict:
        return {
            'bootstrap.servers': ",".join(self.bootstrap_servers),
            'group.id': self.group_id,
            'auto.offset.reset': self.auto_offset_reset,
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 1000,
            'session.timeout.ms': 30000
        }

    @cached_property
    def consumer(self) -> Consumer:
        consumer = Consumer(self.config)
        self.is_running = False
        logger.info(f"✅ Consumer initialized with group ID: {self.group_id}")

        return consumer

def run(
    bootstrap_server_hosts: list[str],
    topics: list[str],
    group_id: str,
    consumer_id: str,
) -> None:
    try:
        subscriber = KafkaConsumer(
            bootstrap_servers=bootstrap_server_hosts,
            group_id=group_id,
            consumer_id=consumer_id,
        )

        # Subscribe to topics
        subscriber.subscribe(topics)

        subscriber.start_consuming()

    except Exception as e:
        logger.error(f"💥 Failed to start consumer: {e}")


def partition_based_consumers(topics: list[str]) -> None:
    """
    Each consumer handles specific partitions for true parallelism
    Using actual confluent_kafka Consumer class
    """

    def create_partition_consumer(consumer_id, partitions):
        consumer = Consumer({
            'bootstrap.servers': 'localhost:29092',
            'group.id': 'partition-group',
            'auto.offset.reset': 'earliest'
        })

        topic_partitions = [
            TopicPartition(topic, p)
            for p in partitions
            for topic in topics
        ]
        consumer.assign(topic_partitions)
        logger.info(f"🎯 [{consumer_id}] Assigned partitions: {partitions}")

        return consumer

    try:
        consumer1 = create_partition_consumer("consumer-1", [0, 5, 6])
        consumer2 = create_partition_consumer("consumer-2", [1, 4, 7])
        consumer3 = create_partition_consumer("consumer-3", [2, 3, 7])

        def process_partition_messages(consumer, consumer_id):

            with duckdb.connect("olist.db") as duck_conn:
                try:
                    while True:
                        msg = consumer.poll(0.1)
                        if msg is None:
                            continue
                        if msg.error():
                            logger.error(f"Consumer error: {msg.error()}")
                            continue

                        values = json.loads(msg.value().decode('utf-8'))
                        logger.info(f"⚡ [{consumer_id}] Partition {msg.partition()}: {values.get('id', 'N/A')}")

                        # Example processing logic
                        handler = HandleOlist(duck_conn)
                        table_name = handler.create_src_table(msg.topic())
                        handler.insert_row_into_table(table_name, values)

                except Exception as e:
                    logger.error(f"❌ [{consumer_id}] Error: {e}")
                finally:
                    consumer.close()

        # Run partition consumers in parallel
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(process_partition_messages, consumer1, "consumer-1"),
                executor.submit(process_partition_messages, consumer2, "consumer-2"),
                executor.submit(process_partition_messages, consumer3, "consumer-3"),
            ]

            try:
                for future in futures:
                    future.result()
            except KeyboardInterrupt:
                logger.info("Partition consumers shutting down...")

    except Exception as e:
        logger.error(f"Failed to setup partition consumers: {e}")


if __name__ == "__main__":
    partition_based_consumers(
        topics=TOPICS,
    )
