#!/bin/bash

# Configuration
MAX_RETRIES=30
RETRY_COUNT=0
RETRY_INTERVAL=5
SERVICE_RETRY_INTERVAL=30
DATA_DIR="/app/data"
RESOURCES_DIR="/app/resources"
MODEL_PATH="$RESOURCES_DIR/sentiment_model_sklearn.pkl"


# Function to log messages with timestamp
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# Wait for a service to be ready
wait_for_service() {
    local host=$1
    local port=$2
    local service=$3
    local retry_count=0
   
    log "Waiting for $service at $host:$port..."
    while [ $retry_count -lt $MAX_RETRIES ]; do
        if nc -z $host $port 2>/dev/null; then
            log "$service is ready!"
            return 0
        fi
        retry_count=$((retry_count + 1))
        log "Retry $retry_count/$MAX_RETRIES..."
        sleep $RETRY_INTERVAL
    done
    log "ERROR: $service not available after $MAX_RETRIES retries"
    return 1
}

# Check if data file exists
check_data_file() {
    if [ ! -f "$DATA_DIR/Data.json" ]; then
        log "ERROR: Training data file not found at $DATA_DIR/Data.json"
        return 1
    fi
    return 0
}

# Check if model file exists
check_model_file() {
    if [ ! -f "$MODEL_PATH" ]; then
        log "ERROR: Model file not found at $MODEL_PATH"
        return 1
    fi
    return 0
}

# Start Spark master in background
log "Starting Spark master..."
/opt/bitnami/spark/sbin/start-master.sh &
SPARK_MASTER_PID=$!

# Wait for Spark master to be ready
log "Waiting for Spark master to be ready..."
wait_for_service "localhost" "8080" "Spark Master"

# Loop for Kafka services
while true; do
    if wait_for_service "kafka1" 9092 "Kafka 1" && wait_for_service "kafka2" 9094 "Kafka 2"; then
        log "Both Kafka services are ready!"
        break
    fi
    log "Waiting for Kafka services to be ready..."
    sleep $SERVICE_RETRY_INTERVAL
done

# Continuously try to get the model ready
while true; do
    if check_model_file; then
        log "Model file exists, proceeding to Spark submit..."
        break
    else
        log "Model file not found, checking for data file..."
        if check_data_file; then
            log "Data file is ready!"
            log "Training the model..."
            
            # Try to train the model
            python3 /app/train_model.py
            
            # Check if training was successful
            if check_model_file; then
                log "Model training completed successfully!"
                break
            else
                log "Model training did not complete successfully, will retry..."
                sleep $RETRY_INTERVAL
            fi
        else
            log "Waiting for data file to be available..."
            sleep $RETRY_INTERVAL
        fi
    fi
done

# Start Spark submit
log "Starting the Spark submit ..."
/opt/bitnami/spark/bin/spark-submit --master local[*] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 --py-files /app/ml_service.py /app/ml_service.py
