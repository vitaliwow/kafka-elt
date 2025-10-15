import json
import logging

import duckdb

from consumers.base import partition_based_consumers
from consumers.handler import HandleOlist
from topics import FACTS_TABLE_TOPICS

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def process_facts_table(consumer, consumer_id):
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
                handler.create_facts_table()

        except Exception as e:
            logger.error(f"❌ [{consumer_id}] Error: {e}")
        finally:
            consumer.close()


if __name__ == "__main__":
    # create source tables
    partition_based_consumers(
        topics=FACTS_TABLE_TOPICS,
        process_partition_messages=process_facts_table,
    )
