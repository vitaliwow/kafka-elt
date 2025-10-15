import logging
import os
import random
import time

import pandas as pd

from producers.base import KafkaPublisher
from topics import SOURCE_TOPICS, FACTS_TABLE_TOPICS

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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


def create_facts(bootstrap_servers, topics):
    pass


if __name__ == "__main__":
    stream_source_csv(
        bootstrap_servers=["localhost:29092", ],
        topics=SOURCE_TOPICS,
    )
    create_facts(
        bootstrap_servers=["localhost:29092", ],
        topics=FACTS_TABLE_TOPICS,
    )