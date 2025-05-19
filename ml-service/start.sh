#!/bin/bash

# Configuration
MAX_RETRIES=30
RETRY_INTERVAL=5
SERVICE_RETRY_INTERVAL=30
DATA_DIR="/app"
RESOURCES_DIR="/app/resources"
MODEL_PATH="$RESOURCES_DIR/sentiment_model_sklearn.pkl"
PYTHON_CMD="python3"

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
    log "Data file found at $DATA_DIR/Data.json"
    return 0
}

# Main process
log "Starting ML service container..."
log "Environment information:"
log "PYSPARK_PYTHON: $PYSPARK_PYTHON"
log "PYSPARK_DRIVER_PYTHON: $PYSPARK_DRIVER_PYTHON"
log "PYTHONPATH: $PYTHONPATH"
log "Python version: $($PYTHON_CMD --version 2>&1)"

# Main loop
while true; do
    log "Starting service check cycle..."
    
    # Check Kafka services
    if ! wait_for_service kafka1 9092 "Kafka1"; then
        log "Kafka1 check failed. Retrying in $SERVICE_RETRY_INTERVAL seconds..."
        sleep $SERVICE_RETRY_INTERVAL
        continue
    fi
    
    if ! wait_for_service kafka2 9094 "Kafka2"; then
        log "Kafka2 check failed. Retrying in $SERVICE_RETRY_INTERVAL seconds..."
        sleep $SERVICE_RETRY_INTERVAL
        continue
    fi

    # Check Spark master
    if ! wait_for_service spark-master 7077 "Spark Master"; then
        log "Spark Master check failed. Retrying in $SERVICE_RETRY_INTERVAL seconds..."
        sleep $SERVICE_RETRY_INTERVAL
        continue
    fi

    # Check if data file exists before attempting training
    if ! check_data_file; then
        log "ERROR: Cannot proceed without training data. Retrying in $SERVICE_RETRY_INTERVAL seconds..."
        sleep $SERVICE_RETRY_INTERVAL
        continue
    fi

    # Check if model exists, if not train it
    if [ ! -f "$MODEL_PATH" ]; then
        log "Model not found at $MODEL_PATH. Starting model training..."
        
        # Create necessary directories
        mkdir -p "$RESOURCES_DIR"
        
        log "Running training script..."
        if ! $PYTHON_CMD $DATA_DIR/train_sklearn_model.py; then
            log "ERROR: Training failed. Retrying in $SERVICE_RETRY_INTERVAL seconds..."
            sleep $SERVICE_RETRY_INTERVAL
            continue
        fi
        log "Model training completed successfully"
    else
        log "Model already exists at $MODEL_PATH, skipping training"
    fi

    # Run ML service
    log "Starting ML service application..."
    if ! $PYTHON_CMD $DATA_DIR/ml_service.py; then
        log "ERROR: ML service failed. Retrying in $SERVICE_RETRY_INTERVAL seconds..."
        sleep $SERVICE_RETRY_INTERVAL
        continue
    fi
    
    log "Service cycle completed successfully"
done