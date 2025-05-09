FROM openjdk:11-jdk-slim

WORKDIR /app

# Install Python and other dependencies in a single layer
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    python3-dev \
    build-essential \
    netcat-traditional \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python packages
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt && \
    python3 -c "import nltk; nltk.download('punkt'); nltk.download('stopwords'); nltk.download('wordnet'); nltk.download('omw-1.4')"



WORKDIR /app

# Install only runtime dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    netcat-traditional \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Create necessary directories
RUN mkdir -p /app/data /app/resources /app/results

# Copy application files
COPY train_sklearn_model.py /app/
COPY ml_service.py /app/
COPY Data.json /app/data/
COPY start.sh /app/

# Make the startup script executable
RUN chmod +x /app/start.sh

# Run the startup script
CMD ["/app/start.sh"]

