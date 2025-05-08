FROM python:3.9-slim

# Set non-root user for security
RUN groupadd -r collector && useradd -r -g collector collector

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    gnupg \
    unzip \
    chromium \
    chromium-driver \
    netcat-traditional \
    && wget -qO - https://packages.confluent.io/deb/7.3/archive.key | apt-key add - \
    && echo "deb [arch=amd64] https://packages.confluent.io/deb/7.3 stable main" >> /etc/apt/sources.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
    librdkafka-dev \
    kafkacat \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables for Chrome
ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMEDRIVER_PATH=/usr/bin/chromedriver

# Copy requirements file first to leverage Docker cache
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the script
COPY amazon_review_collector.py .

# Create data directory for logs
RUN mkdir -p /data/logs && \
    chown -R collector:collector /data

# Make script executable
RUN chmod +x amazon_review_collector.py && \
    chown -R collector:collector /app

# Set environment variables for production
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Switch to non-root user
USER collector

# Health check to verify the Python process is running
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD ["python", "health_check.py"]
  

# The command will be specified in docker-compose.yml
CMD ["python", "./amazon_review_collector.py"]