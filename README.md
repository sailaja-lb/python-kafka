# Python Kafka Producer and Consumer

A simple Python project demonstrating Kafka producer and consumer implementations using the `kafka-python` library.

## Project Structure

```
Python-kafka/
├── src/
│   ├── __init__.py              # Package initialization
│   ├── config.py                # Configuration settings
│   ├── producer.py              # Kafka Producer implementation
│   └── consumer.py              # Kafka Consumer implementation
├── requirements.txt             # Python dependencies
├── .env.example                 # Example environment variables
└── README.md                    # This file
```

## Features

### Producer
- Sends messages to Kafka topics
- JSON serialization support
- Error handling and retries
- Batch message sending
- Configurable broker and topic settings

### Consumer
- Consumes messages from Kafka topics
- JSON deserialization support
- Consumer group support
- Batch message consumption
- Message processing with custom functions
- Error handling

## Prerequisites

- Python 3.7 or higher
- Kafka broker running (default: `localhost:9092`)
- pip (Python package manager)

## Installation

1. Clone or navigate to the project directory:
   ```bash
   cd Python-kafka
   ```

2. Create a virtual environment (optional but recommended):
   ```bash
   # On Windows
   python -m venv venv
   venv\Scripts\activate
   
   # On macOS/Linux
   python -m venv venv
   source venv/bin/activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Create a `.env` file from the example:
   ```bash
   cp .env.example .env
   ```

   Edit `.env` to match your Kafka broker configuration (if needed).

## Configuration

Edit the `.env` file to configure:
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka broker address (default: `localhost:9092`)
- `KAFKA_TOPIC`: Topic to produce/consume from (default: `my-topic`)
- `CONSUMER_GROUP_ID`: Consumer group identifier (default: `my-consumer-group`)
- Consumer and Producer settings

## Usage

### Using as a Library

```python
from src.producer import KafkaProducerClient
from src.consumer import KafkaConsumerClient

# Producer example
producer = KafkaProducerClient()
producer.send_message({"user_id": 1, "action": "login"})
producer.close()

# Consumer example
consumer = KafkaConsumerClient()
def process_msg(msg):
    print(f"Received: {msg}")

consumer.process_messages(process_msg)
consumer.close()
```

### Running Examples

1. **Producer Example**:
   ```bash
   python -m src.producer
   ```

2. **Consumer Example**:
   ```bash
   python -m src.consumer
   ```

## Setting up Kafka Locally

### Using Docker Compose

If you have Docker and Docker Compose installed, create a `docker-compose.yml`:

```yaml
version: '3.8'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```

Then run:
```bash
docker-compose up -d
```

## API Reference

### KafkaProducerClient

#### Methods

- `send_message(message, key=None)` - Send a single message
- `send_batch(messages)` - Send multiple messages
- `close()` - Close the producer connection

### KafkaConsumerClient

#### Methods

- `consume_messages(timeout=None)` - Consume all available messages
- `consume_single_batch(max_records=100, timeout_ms=1000)` - Consume a batch
- `process_messages(process_func)` - Process messages with a custom function
- `close()` - Close the consumer connection

## Troubleshooting

### Connection Issues
- Ensure Kafka broker is running on the configured address
- Check `KAFKA_BOOTSTRAP_SERVERS` in `.env`

### No Messages Received
- Verify messages are being sent to the correct topic
- Check consumer group ID matches if expecting historical messages
- Use `auto_offset_reset=earliest` to read from the beginning

### Module Import Errors
- Ensure dependencies are installed: `pip install -r requirements.txt`
- Add the project root to PYTHONPATH if needed

## License

This project is open source and available under the MIT License.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
