import json
import logging
import signal
import typing as t
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property

from confluent_kafka import Consumer, KafkaError, KafkaException

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
        logger.info(f"📝 Subscribed to topics: {topics}")

    def process_message(self, message):
        """Process a single message - override this for custom logic"""
        try:
            # Decode the message
            value = json.loads(message.value().decode('utf-8'))
            key = message.key().decode('utf-8') if message.key() else None

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
        logger.info("🎧 Starting to consume messages...")

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
                        logger.error(f"Consumer error: {msg.error()}")
                else:
                    self.process_message(msg)

        except KeyboardInterrupt:
            logger.info("⏹️ Consumer interrupted by user")
        except Exception as e:
            logger.error(f"💥 Consumer error: {e}")
        finally:
            self.close()

    def close(self):
        """Close the consumer connection"""
        self.consumer.close()
        logger.info(f"🔴 Consumer closed. Total messages processed: {self.message_count}")

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

        # Setup graceful shutdown
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        return consumer

def run(
    bootstrap_server_hosts: list[str],
    topics: list[str],
    group_id: str,
) -> None:
    try:
        subscriber = KafkaConsumer(
            bootstrap_servers=bootstrap_server_hosts,
            group_id=group_id,
        )

        # Subscribe to topics
        subscriber.subscribe(topics)

        subscriber.start_consuming()

    except Exception as e:
        logger.error(f"💥 Failed to start consumer: {e}")


if __name__ == "__main__":
    run(
        bootstrap_server_hosts=[
            "localhost:29092",
        ],
        topics=[
            "source-csv",
        ],
        group_id="main-consumer-group",
    )
