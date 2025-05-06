#!/usr/bin/env python3
# health_check.py

import os
import subprocess
import sys

def check_process_running():
    try:
        # Check if the collector process is running
        result = subprocess.run(
            ["ps", "aux"], 
            capture_output=True, 
            text=True
        )
        
        if "amazon_review_collector.py" not in result.stdout:
            print("Error: amazon_review_collector.py process not found")
            return False
        return True
    except Exception as e:
        print(f"Process check failed: {e}")
        return False

def check_kafka_connection():
    try:
        # Import here to avoid import errors affecting the process check
        from kafka import KafkaProducer
        
        # Get Kafka bootstrap servers from environment
        bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka1:9092,kafka2:9094')
        
        # Try to create a producer (this will fail if Kafka is unreachable)
        producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
        producer.close()
        return True
    except Exception as e:
        print(f"Kafka connection check failed: {e}")
        return False

if __name__ == "__main__":
    process_ok = check_process_running()
    kafka_ok = check_kafka_connection()
    
    if process_ok and kafka_ok:
        sys.exit(0)  # Both checks passed
    else:
        sys.exit(1)  # At least one check failed