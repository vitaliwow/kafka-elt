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

                data = json.loads(msg.value().decode('utf-8'))
                logger.info(f"⚡ [{consumer_id}] Partition {msg.partition()}")

                # Example processing logic
                handler = HandleOlist(duck_conn)
                facts_table_name = handler.create_facts_table()
                data_field_values = data.get("data", {})
                facts_data = {
                    "order_id": data_field_values.get("order_id"),
                    "order_item_id": data_field_values.get("order_item_id"),
                    "product_id": data_field_values.get("product_id"),
                    "seller_id": data_field_values.get("seller_id"),
                    "customer_unique_id": data_field_values.get("customer_unique_id"),
                    "price": data_field_values.get("price"),
                }
                handler.insert_row_into_table(facts_table_name, facts_data)

        except Exception as e:
            logger.error(f"❌ [{consumer_id}] Error: {e}")
        finally:
            consumer.close()


if __name__ == "__main__":
    # create source tables
    partition_based_consumers(
        topics=["olist_order_items_dataset", ],
        process_partition_messages=process_source_tables,
    )
