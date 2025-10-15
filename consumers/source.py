import json
import logging

import duckdb

from consumers.base import partition_based_consumers
from consumers.handler import HandleOlist
from topics import SOURCE_TOPICS

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def process_source_tables(consumer, consumer_id):
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


if __name__ == "__main__":
    # create source tables
    partition_based_consumers(
        topics=SOURCE_TOPICS,
        process_partition_messages=process_source_tables,
    )
