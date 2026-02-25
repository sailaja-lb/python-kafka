"""
Kafka Producer - sends messages to a Kafka topic
"""
import json
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError
from .config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    PRODUCER_ACKS,
    PRODUCER_RETRIES,
    PRODUCER_BATCH_SIZE
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class KafkaProducerClient:
    """Kafka Producer Client wrapper"""
    
    def __init__(self, topic=KAFKA_TOPIC, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS):
        """
        Initialize the Kafka Producer
        
        Args:
            topic: The topic to send messages to
            bootstrap_servers: Kafka broker addresses
        """
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            acks=PRODUCER_ACKS,
            retries=PRODUCER_RETRIES,
            batch_size=PRODUCER_BATCH_SIZE
            # No value_serializer - we handle serialization in send_message()
        )
        logger.info(f"Producer initialized for topic: {topic}")
    
    def send_message(self, message, key=None):
        """
        Send a message to the Kafka topic
        
        Args:
            message: The message to send (dict will be JSON serialized, str sent as-is)
            key: Optional message key
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Handle different message types
            if isinstance(message, dict):
                # Serialize dict to JSON
                value = json.dumps(message).encode('utf-8')
            elif isinstance(message, str):
                # Send string as-is (for malformed/raw data testing)
                value = message.encode('utf-8')
            else:
                # Convert other types to JSON
                value = json.dumps(message).encode('utf-8')
            
            future = self.producer.send(
                self.topic,
                value=value,
                key=key.encode('utf-8') if key else None
            )
            record_metadata = future.get(timeout=10)
            
            logger.info(
                f"Message sent to topic={record_metadata.topic}, "
                f"partition={record_metadata.partition}, "
                f"offset={record_metadata.offset}"
            )
            return True
        except KafkaError as e:
            logger.error(f"Failed to send message: {e}")
            return False
    
    def send_raw_message(self, raw_message, key=None):
        """
        Send a raw message without JSON serialization (for testing malformed data)
        
        Args:
            raw_message: Raw string to send as-is
            key: Optional message key
            
        Returns:
            bool: True if successful, False otherwise
        """
        return self.send_message(raw_message, key)
    
    def send_batch(self, messages):
        """
        Send multiple messages to the Kafka topic
        
        Args:
            messages: List of messages to send
            
        Returns:
            int: Number of messages sent successfully
        """
        sent_count = 0
        for message in messages:
            if self.send_message(message):
                sent_count += 1
        return sent_count
    
    def close(self):
        """Close the producer connection"""
        self.producer.flush()
        self.producer.close()
        logger.info("Producer closed")


if __name__ == "__main__":
    # Example usage
    producer = KafkaProducerClient(topic="transactions-topic")
    
    print("\n=== Sending Valid JSON Messages ===")
    # Send valid JSON objects (dicts) - Producer will JSON serialize them
    # valid_messages = [
    #     {"user_id": 1, "action": "login", "timestamp": "2024-01-01T10:00:00"},
    #     {"user_id": 2, "action": "purchase", "timestamp": "2024-01-01T10:01:00"},
    #     {"user_id": 3, "action": "logout", "timestamp": "2024-01-01T10:02:00"},
    # ]
    valid_messages = [
         {"transaction_id": 1002, "customer_id": 202, "product_id": 501, "date": "2025-02-15", "quantity": 5, "amount":  149.99}
        
        
        
    ]
    
    for msg in valid_messages:
        producer.send_message(msg)
        print(f"OK: {msg}")
    
    # print("\n=== Sending Malformed JSON Messages (for DLQ testing) ===")
    # Send malformed/incomplete JSON strings (will be rejected by consumer)
    # malformed_messages = [
    #     "{incomplete",  # Missing closing brace
    #     '{"missing": "quote}',  # Missing closing quote
    #     "[array, not, object]",  # Array instead of object
    #     "not json at all",  # Plain text
    # ]
    
    # for msg in malformed_messages:
    #     producer.send_raw_message(msg)
    #     print(f"ERROR: {msg}")
    
    print("\n")
    producer.close()
