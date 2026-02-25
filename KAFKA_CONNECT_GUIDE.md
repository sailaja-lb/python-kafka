# Kafka Connect Setup Guide

## Starting Kafka Connect

1. **Restart Docker Compose** with the updated configuration:
   ```bash
   docker compose down
   docker compose up -d
   ```

2. **Verify Kafka Connect is running**:
   ```bash
   curl http://localhost:8083/
   ```
   Should return Kafka Connect version info.

## Installing S3 Connector

### Option A: Using Confluent Cloud (Recommended for S3)

The Confluent S3 Sink Connector requires a paid license. Instead, use the open-source **Aiven S3 Sink Connector**:

```bash
# Download the connector
docker exec kafka-connect curl -o /etc/kafka-connect/jars/aiven-kafka-connect-s3-0.14.0.jar \
  https://github.com/Aiven-Open/s3-connector-for-apache-kafka/releases/download/v0.14.0/aiven-kafka-connect-s3-0.14.0.jar

# Restart Kafka Connect
docker restart kafka-connect
```

### Option B: Using Simple File Connector (Testing)

You can use Kafka's built-in file connector to test:

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @file-sink-connector.json
```

## Create a Connector

### Using REST API

```bash
# Create a connector
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "test-file-sink",
    "config": {
      "connector.class": "org.apache.kafka.connect.file.FileStreamSinkConnector",
      "tasks.max": "1",
      "topics": "test-topic",
      "file": "/tmp/test-sink.txt"
    }
  }'
```

### Check Connector Status

```bash
# List all connectors
curl http://localhost:8083/connectors

# Get specific connector status
curl http://localhost:8083/connectors/test-file-sink/status

# Get connector config
curl http://localhost:8083/connectors/test-file-sink/config
```

### Stop/Delete a Connector

```bash
# Pause connector
curl -X PUT http://localhost:8083/connectors/test-file-sink/pause

# Resume connector
curl -X PUT http://localhost:8083/connectors/test-file-sink/resume

# Delete connector
curl -X DELETE http://localhost:8083/connectors/test-file-sink
```

## Connecting to S3 (Production)

For production S3 integration, you have options:

1. **Aiven S3 Connector** (Open source, free)
   - GitHub: https://github.com/Aiven-Open/s3-connector-for-apache-kafka
   - Works with any S3-compatible storage

2. **AWS Kinesis Firehose**
   - Use Kafka Connect to send to Firehose, which then delivers to S3

3. **Custom Python Solution**
   - Build a consumer that directly writes to S3 using boto3

## Example: File Sink Connector

To test basic connectivity, use the File Sink Connector:

```json
{
  "name": "test-file-sink",
  "config": {
    "connector.class": "org.apache.kafka.connect.file.FileStreamSinkConnector",
    "tasks.max": "1",
    "topics": "test-topic",
    "file": "/tmp/kafka-sink.txt"
  }
}
```

## Python Consumer Alternative

Instead of Kafka Connect, you can use a Python consumer to write to S3:

```python
from kafka import KafkaConsumer
import boto3
import json

consumer = KafkaConsumer(
    'test-topic',
    bootstrap_servers=['localhost:9092']
)

s3 = boto3.client('s3')

for message in consumer:
    data = json.loads(message.value.decode('utf-8'))
    s3.put_object(
        Bucket='my-bucket',
        Key=f'messages/{message.offset}.json',
        Body=json.dumps(data)
    )
```

## Monitoring

- **Kafka Connect REST API**: http://localhost:8083
- **Kafka UI**: http://localhost:8080
- **Check connector logs**: `docker logs kafka-connect`
