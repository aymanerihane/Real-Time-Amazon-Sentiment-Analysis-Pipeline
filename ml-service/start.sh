#!/bin/bash
# Configuration
MAX_RETRIES=30
RETRY_COUNT=0
RETRY_INTERVAL=5
SERVICE_RETRY_INTERVAL=30
DATA_DIR="/app/data"
RESOURCES_DIR="/app/resources"
MODEL_PATH="$RESOURCES_DIR/sentiment_model_sklearn.pkl"
PYTHON_CMD="python3"  # Use python3 explicitly

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

# Main process
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
        
        if ! $PYTHON_CMD train_sklearn_model.py; then
            log "ERROR: Training failed. Retrying in $SERVICE_RETRY_INTERVAL seconds..."
            sleep $SERVICE_RETRY_INTERVAL
            continue
        fi
        log "Model training completed successfully"
    else
        log "Model already exists at $MODEL_PATH, skipping training"
    fi

    # Check Spark
    if ! wait_for_service spark-master 7077 "Spark"; then
        log "Spark check failed. Retrying in $SERVICE_RETRY_INTERVAL seconds..."
        sleep $SERVICE_RETRY_INTERVAL
        continue
    fi

    # Run ML service
    log "Starting ML service..."
    if ! $PYTHON_CMD ml_service.py; then
        log "ERROR: ML service failed. Retrying in $SERVICE_RETRY_INTERVAL seconds..."
        sleep $SERVICE_RETRY_INTERVAL
        continue
    fi
   
    log "Service cycle completed successfully"
done