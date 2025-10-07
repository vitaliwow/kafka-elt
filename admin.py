from confluent_kafka.admin import AdminClient, NewTopic
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class KafkaAdmin:
    def __init__(self, bootstrap_servers='localhost:29092'):
        self.conf = {
            'bootstrap.servers': bootstrap_servers
        }
        self.admin_client = AdminClient(self.conf)
        logger.info(f"✅ Admin client connected to Kafka at {bootstrap_servers}")

    def create_topic(self, topic_name, num_partitions=1, replication_factor=1):
        """Create a new topic"""
        topic = NewTopic(
            topic=topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor
        )

        try:
            # Create topics returns a dict of futures
            futures = self.admin_client.create_topics([topic])

            # Wait for operation to complete
            for topic_name, future in futures.items():
                try:
                    future.result()  # Wait for the result
                    logger.info(f"✅ Topic '{topic_name}' created successfully")
                    return True
                except Exception as e:
                    if "TOPIC_ALREADY_EXISTS" in str(e):
                        logger.info(f"ℹ️  Topic '{topic_name}' already exists")
                        return True
                    else:
                        logger.error(f"❌ Error creating topic '{topic_name}': {e}")
                        return False
        except Exception as e:
            logger.error(f"❌ Error creating topic: {e}")
            return False

    def list_topics(self):
        """List all topics"""
        try:
            metadata = self.admin_client.list_topics(timeout=10)
            topics = metadata.topics
            logger.info("📋 Available topics:")
            for topic_name, topic_metadata in topics.items():
                logger.info(f"  - {topic_name} (partitions: {len(topic_metadata.partitions)})")
            return list(topics.keys())
        except Exception as e:
            logger.error(f"❌ Error listing topics: {e}")
            return []

    def delete_topic(self, topic_name):
        """Delete a topic"""
        try:
            futures = self.admin_client.delete_topics([topic_name])

            for topic_name, future in futures.items():
                try:
                    future.result()
                    logger.info(f"✅ Topic '{topic_name}' deleted successfully")
                    return True
                except Exception as e:
                    logger.error(f"❌ Error deleting topic '{topic_name}': {e}")
                    return False
        except Exception as e:
            logger.error(f"❌ Error deleting topic: {e}")
            return False


if __name__ == "__main__":
    try:
        admin = KafkaAdmin()

        # Create some example topics
        topics_to_create = ["source-csv"]

        for topic in topics_to_create:
            admin.create_topic(topic, num_partitions=3, replication_factor=1)

        # List all topics
        admin.list_topics()

    except Exception as e:
        logger.error(f"💥 Admin operations failed: {e}")