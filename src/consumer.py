"""
Kafka Consumer - consumes messages from a Kafka topic
"""
import json
import logging
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from .config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    CONSUMER_GROUP_ID,
    CONSUMER_AUTO_OFFSET_RESET
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class KafkaConsumerClient:
    """Kafka Consumer Client wrapper"""
    
    def __init__(self, topic=KAFKA_TOPIC, group_id=CONSUMER_GROUP_ID, 
                 bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS):
        """
        Initialize the Kafka Consumer
        
        Args:
            topic: The topic to consume from
            group_id: Consumer group ID
            bootstrap_servers: Kafka broker addresses
        """
        self.topic = topic
        self.group_id = group_id

        # DLQ producer 
        self.dlq_producer = KafkaProducer( 
            bootstrap_servers=bootstrap_servers, 
            value_serializer=lambda v: json.dumps(v).encode('utf-8') 
            )
        
        # IMPORTANT: remove value_deserializer
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=None,  # Disable consumer groups
            auto_offset_reset=CONSUMER_AUTO_OFFSET_RESET,
            enable_auto_commit=False,
            # value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=1000
        )
        logger.info(f"Consumer initialized for topic: {topic}, group: {group_id}")
    
    def consume_messages(self):
        """Consume messages with DLQ handling""" 
        try: 
            for message in self.consumer: 
                raw = message.value.decode("utf-8") 
                logger.debug(f"Raw message: {repr(raw)}")  # Debug output
                
                try: 
                    parsed = json.loads(raw) 
                    logger.info(f"VALID JSON: {parsed}") 
                    print(f"[OK] Valid JSON: {parsed}")
                    
                except (json.JSONDecodeError, ValueError) as e: 
                    logger.error(f"INVALID JSON: {repr(raw)} | Error: {e}") 
                    print(f"[ERROR] Invalid JSON: {repr(raw)}")
                    
                    # Send to DLQ 
                    self.dlq_producer.send("test-topic-dlq", value={"error": str(type(e).__name__), "data": raw}) 
                    self.dlq_producer.flush() 
                    logger.info("→ Routed to DLQ")
                    print("[DLQ] Sent to Dead Letter Queue")
                    
        except KafkaError as e: 
            logger.error(f"Kafka error: {e}")
        
    def close(self): 
        self.consumer.close()
        self.dlq_producer.close()
        logger.info("Consumer and DLQ producer closed")





    
#     def consume_messages(self, timeout=None):
#         """
#          Consume messages from the Kafka topic
        
#         Args:
#             timeout: Optional timeout in milliseconds
            
#         Returns:
#             list: List of consumed messages
#         """
#         messages = []
#         try:
#             for message in self.consumer:
#                 logger.info(
#                     f"Received message - Partition: {message.partition}, "
#                     f"Offset: {message.offset}, Value: {message.value}"
#                 )
#                 messages.append(message.value)
#         except KafkaError as e:
#             logger.error(f"Error consuming messages: {e}")
        
#         return messages
    
#     def consume_single_batch(self, max_records=100, timeout_ms=1000):
#         """
#         Consume a single batch of messages
        
#         Args:
#             max_records: Maximum records to fetch in one poll
#             timeout_ms: Timeout in milliseconds
            
#         Returns:
#             dict: Raw consumer records
#         """
#         try:
#             records = self.consumer.poll(timeout_ms=timeout_ms, max_records=max_records)
#             return records
#         except KafkaError as e:
#             logger.error(f"Error consuming batch: {e}")
#             return {}
    
#     def process_messages(self, process_func):
#         """
#         Consume messages and apply a processing function to each
        
#         Args:
#             process_func: Function to apply to each message
#         """
#         try:
#             for message in self.consumer:
#                 logger.info(f"Processing message: {message.value}")
#                 process_func(message.value)
#         except KafkaError as e:
#             logger.error(f"Error processing messages: {e}")
    
#     def close(self):
#         """Close the consumer connection"""
#         self.consumer.close()
#         logger.info("Consumer closed")


if __name__ == "__main__":
    # Example usage
    consumer = KafkaConsumerClient()
    
    # Define a processing function
    def process_message(message):
        print(f"Processing: {message}")
    
    # Consume and process messages
    print("Starting to consume messages... (waiting for messages)")
    consumer.consume_messages()
    
    consumer.close()
