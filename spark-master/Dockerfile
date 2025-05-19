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

# Get Python version for reference
RUN python3 --version

# Create app directory
WORKDIR /app

# Copy and fix requirements
COPY requirements.txt /app/requirements.txt

# Install Python dependencies with specific version constraints for Python 3.12 compatibility
RUN pip3 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir setuptools wheel && \
    pip3 install --no-cache-dir -r /app/requirements.txt

# Download NLTK data
# RUN python3 -c "import nltk; nltk.download('punkt'); nltk.download('stopwords'); nltk.download('wordnet'); nltk.download('omw-1.4')"

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

# Add Kafka JARs to Spark classpath
RUN mkdir -p /opt/bitnami/spark/jars
RUN wget -P /opt/bitnami/spark/jars https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.1.3/spark-sql-kafka-0-10_2.12-3.1.3.jar && \
    wget -P /opt/bitnami/spark/jars https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/2.8.1/kafka-clients-2.8.1.jar

# Set environment variables
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV PYTHONPATH=/app:$PYTHONPATH
ENV PATH="/app:${PATH}"

# Switch back to non-root user
USER 1001

# Run the startup script
CMD ["/app/start.sh"]