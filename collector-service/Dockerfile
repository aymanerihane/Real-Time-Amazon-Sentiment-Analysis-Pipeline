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
  

# Use ENTRYPOINT with CMD for better container orchestration
ENTRYPOINT ["python"]
CMD ["./amazon_review_collector.py"]