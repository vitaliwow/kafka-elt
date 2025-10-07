markdown
# Kafka Pub/Sub with kafka-python

A simple Pub/Sub implementation using Kafka and kafka-python.

## Quick Start

1. **Start Kafka:**
   ```bash
   docker-compose up -d

2. **Install dependencies:**
   ```bash
    pip install -r requirements.txt

3. **Create topics (optional):**
   ```bash
    python admin.py

4. **Run consumer (terminal 1):**
    ```bash
    python consumer.py

5. **Run producer (terminal 2):**
    ```bash
    python producer.py
