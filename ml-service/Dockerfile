FROM openjdk:11-jdk-slim

WORKDIR /app

# Install Python 3.11 and other dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    software-properties-common \
    build-essential \
    curl \
    zlib1g-dev \
    libssl-dev \
    libffi-dev \
    libsqlite3-dev \
    libbz2-dev \
    && curl -sS https://www.python.org/ftp/python/3.11.0/Python-3.11.0.tgz | tar xz \
    && cd Python-3.11.0 \
    && ./configure --enable-optimizations --enable-loadable-sqlite-extensions \
    && make -j $(nproc) \
    && make altinstall \
    && cd .. \
    && rm -rf Python-3.11.0 \
    && ln -sf /usr/local/bin/python3.11 /usr/local/bin/python3 \
    && ln -sf /usr/local/bin/pip3.11 /usr/local/bin/pip3 \
    && apt-get install -y --no-install-recommends \
    python3-pip \
    python3-dev \
    netcat-traditional \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Create necessary directories
RUN mkdir -p /app/data /app/resources /app/results

# Copy requirements and install Python packages
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt && \
    python3 -c "import nltk; nltk.download('punkt'); nltk.download('stopwords'); nltk.download('wordnet'); nltk.download('omw-1.4')"

# Copy application files
COPY train_sklearn_model.py /app/
COPY ml_service.py /app/
COPY Data.json /app/data/
COPY start.sh /app/

# Display Python version for verification
RUN python3 --version

# Make the startup script executable
RUN chmod +x /app/start.sh

# Set environment variables for Python paths
ENV PYSPARK_PYTHON=/usr/local/bin/python3.11
ENV PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.11

# Run the startup script
CMD ["/app/start.sh"]