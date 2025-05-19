FROM bitnami/spark:latest

# Switch to root for installation
USER root

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    wget \
    netcat-traditional \
    python3-pip \
    python3-setuptools \
    dos2unix \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Copy requirements file and install dependencies
COPY requirements.txt /app/
RUN pip3 install --no-cache-dir -r /app/requirements.txt

# Copy application files
COPY train_sklearn_model.py /app/
COPY ml_service.py /app/
COPY Data.json /app/
COPY log4j.properties /app/

# Create directories for model
RUN mkdir -p /app/resources

# Special handling for start.sh to ensure proper line endings
COPY start.sh /app/start.sh.original
RUN dos2unix /app/start.sh.original && \
    mv /app/start.sh.original /app/start.sh && \
    chmod +x /app/start.sh

# Add Kafka JARs to Spark classpath
RUN mkdir -p /opt/bitnami/spark/jars
RUN wget -P /opt/bitnami/spark/jars https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.1.3/spark-sql-kafka-0-10_2.12-3.1.3.jar && \
    wget -P /opt/bitnami/spark/jars https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/2.8.1/kafka-clients-2.8.1.jar

# Configure Java options for Spark
ENV SPARK_EXTRA_JAVA_OPTS="-Dlog4j.configuration=file:///app/log4j.properties"

# Set environment variables
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV PYTHONPATH=/app:$PYTHONPATH

# Switch back to non-root user for security
USER 1001

# Command to run the service
CMD ["/app/start.sh"]