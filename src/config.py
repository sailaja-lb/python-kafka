"""
Configuration settings for Kafka producer and consumer
"""
import os
from dotenv import load_dotenv

load_dotenv()

# Kafka broker settings
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'my-topic')

# Producer settings
PRODUCER_ACKS = os.getenv('PRODUCER_ACKS', 'all')
PRODUCER_RETRIES = int(os.getenv('PRODUCER_RETRIES', '3'))
PRODUCER_BATCH_SIZE = int(os.getenv('PRODUCER_BATCH_SIZE', '16384'))

# Consumer settings
CONSUMER_GROUP_ID = os.getenv('CONSUMER_GROUP_ID', 'my-consumer-group')
CONSUMER_AUTO_OFFSET_RESET = os.getenv('CONSUMER_AUTO_OFFSET_RESET', 'earliest')
