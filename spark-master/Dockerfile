FROM bitnami/spark:latest

# Install system dependencies
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    wget \
    netcat-traditional \
    python3-pip \
    python3-setuptools \
    dos2unix \
    && rm -rf /var/lib/apt/lists/*

# Get Spark version for correct package compatibility
RUN echo "Spark version: $(spark-submit --version 2>&1 | grep 'version' | head -1)" > /spark_version.txt
RUN cat /spark_version.txt

# Create app directory
WORKDIR /app

# Copy and fix requirements
COPY requirements.txt /opt/bitnami/spark/requirements.txt

# Explicitly install pandas and other essential packages
RUN pip3 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir setuptools wheel && \
    pip3 install --no-cache-dir pandas numpy nltk pymongo joblib && \
    pip3 install --no-cache-dir -r /opt/bitnami/spark/requirements.txt

# Create NLTK data directory and set permissions
RUN mkdir -p /opt/nltk_data && \
    chmod -R 777 /opt/nltk_data

# Download NLTK data
# RUN python3 -c "import nltk; nltk.data.path.append('/opt/nltk_data'); nltk.download('punkt', download_dir='/opt/nltk_data'); nltk.download('stopwords', download_dir='/opt/nltk_data'); nltk.download('wordnet', download_dir='/opt/nltk_data')"

# Copy application files
COPY train_model.py /app/
COPY ml_service.py /app/
COPY Data.json /app/
COPY start.sh /app/start.sh

# Create resources directory and copy model
RUN mkdir -p /app/resources
COPY resources/sentiment_model_sklearn.pkl /app/resources/

# Fix line endings and make the startup script executable
RUN dos2unix /app/start.sh && \
    chmod +x /app/start.sh

# Create script to set up worker environment
# RUN chmod +x /opt/bitnami/spark/sbin/spark-worker-setup.sh

# Add Kafka JARs to Spark classpath - Using the exact same version as Spark 3.5.x
RUN mkdir -p /opt/bitnami/spark/jars
RUN SPARK_VERSION=$(cat /spark_version.txt | grep -oP '(?<=version )[0-9]+\.[0-9]+\.[0-9]+') && \
    echo "Using Spark version: $SPARK_VERSION for dependencies" && \
    MAJOR_MINOR=$(echo $SPARK_VERSION | cut -d. -f1,2) && \
    wget -P /opt/bitnami/spark/jars https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar && \
    wget -P /opt/bitnami/spark/jars https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar && \
    wget -P /opt/bitnami/spark/jars https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    wget -P /opt/bitnami/spark/jars https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar && \
    ls -la /opt/bitnami/spark/jars/

# Set environment variables
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV PYTHONPATH=/app:$PYTHONPATH
ENV PATH="/app:${PATH}"
ENV SPARK_CLASSPATH="/opt/bitnami/spark/jars/*"

# Set Spark configuration for dependencies
ENV PYSPARK_SUBMIT_ARGS="--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 pyspark-shell"

# Switch back to non-root user
USER 1001

# Run the startup script
CMD ["/app/start.sh"]
