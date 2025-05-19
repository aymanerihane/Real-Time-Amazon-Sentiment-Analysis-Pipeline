#!/bin/bash

# Configuration
MAX_RETRIES=30
RETRY_INTERVAL=5
SERVICE_RETRY_INTERVAL=30
APP_DIR="/app"
RESOURCES_DIR="/app/resources"
MODEL_PATH="$RESOURCES_DIR/sentiment_model_sklearn.pkl"
SPARK_JARS_DIR="/opt/bitnami/spark/jars"


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

# Check if model file exists
check_model_file() {
    if [ ! -f "$MODEL_PATH" ]; then
        log "ERROR: Model file not found at $MODEL_PATH"
        return 1
    fi
    log "Model file found at $MODEL_PATH"
    return 0
}

# Start of script
log "Starting ML service..."
log "Environment information:"
log "Python version: $(python3 --version)"
log "PYSPARK_PYTHON: $PYSPARK_PYTHON"
log "PYSPARK_DRIVER_PYTHON: $PYSPARK_DRIVER_PYTHON"
log "Current working directory: $(pwd)"

# Wait for Kafka services to be ready
log "Waiting for Kafka services..."
wait_for_service "kafka1" 9092 "Kafka1" || log "Warning: Kafka1 not reachable but continuing anyway"
wait_for_service "kafka2" 9094 "Kafka2" || log "Warning: Kafka2 not reachable but continuing anyway"
log "Kafka checks completed"

# Check if model exists
if ! check_model_file; then
    log "Model not found. Will attempt to train it."
    if [ -f "$APP_DIR/train_model.py" ]; then
        log "Running model training script..."
        python3 $APP_DIR/train_model.py
    else
        log "ERROR: Training script not found at $APP_DIR/train_model.py"
    fi
    # Check again
    check_model_file
fi

# List the JARs to ensure they're available
log "Listing available JARs:"
ls -la $SPARK_JARS_DIR/

# Set environment variable for Spark to find the JARs
export SPARK_CLASSPATH="$SPARK_JARS_DIR/*"

# Start the Spark master service in the background
log "Starting Spark master service..."
/opt/bitnami/spark/sbin/start-master.sh &

# Wait a moment for the service to start
sleep 5

# Wait for Spark master to be ready with timeout
log "Waiting for local Spark Master..."
wait_for_service "spark-master" 7077 "Spark Master" || log "Warning: Local Spark Master not reachable but continuing anyway" 

# Sleep a bit to make sure services are fully initialized
sleep 5
log "Preparing to submit Spark job..."

# Start Spark submit with extra class path and proper packages - use nohup to avoid script issues
log "Starting the Spark submission process..."
log "Command: /opt/bitnami/spark/bin/spark-submit --master local[*] ..."

# Run in foreground to avoid process getting killed
/opt/bitnami/spark/bin/spark-submit \
    --master local[*] \
    --jars $SPARK_JARS_DIR/spark-sql-kafka-0-10_2.12-3.5.5.jar,$SPARK_JARS_DIR/kafka-clients-2.8.1.jar \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,org.apache.kafka:kafka-clients:2.8.1 \
    --conf "spark.driver.extraClassPath=$SPARK_JARS_DIR/*" \
    --conf "spark.executor.extraClassPath=$SPARK_JARS_DIR/*" \
    $APP_DIR/ml_service.py
