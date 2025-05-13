import os
import json
from kafka import KafkaConsumer

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka1:9092,kafka2:9094')
KAFKA_SENTIMENT_TOPIC = os.getenv('KAFKA_TOPIC_2', 'sentiment-results')

# Create consumers

sentiment_consumer = KafkaConsumer(
    KAFKA_SENTIMENT_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(','),
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='latest',
    enable_auto_commit=True
)

def consume_messages():
    """Consume raw review messages from Kafka"""
    try:
        message = next(sentiment_consumer, None)
        if message:
            return message.value
    except Exception as e:
        print(f"Error consuming raw review message: {e}")
    return None
