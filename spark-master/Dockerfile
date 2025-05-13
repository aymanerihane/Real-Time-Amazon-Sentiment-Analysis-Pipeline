FROM bitnami/spark:3.4.1

USER root

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    python3-pip \
    python3-dev \
    netcat-traditional \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Create necessary directories with proper permissions
RUN mkdir -p /app/resources /app/data && \
    chown -R 1001:1001 /app && \
    chmod -R 755 /app

# Install Python packages in specific order
COPY requirements.txt .
COPY start.sh /app/
COPY ml_service.py /app/
COPY train_model.py /app/
COPY Data.json /app/data

# Make start.sh executable
RUN chmod +x /app/start.sh

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install pandas in the system Python environment
RUN /opt/bitnami/python/bin/pip install --no-cache-dir pandas==2.0.3 numpy==1.24.3

# Install NLTK and download required data to a shared location
RUN mkdir -p /opt/nltk_data && \
    python3 -c "import nltk; \
    nltk.data.path.append('/nltk_data'); \
    nltk.download('punkt', download_dir='/nltk_data'); \
    nltk.download('stopwords', download_dir='/nltk_data'); \
    nltk.download('wordnet', download_dir='/nltk_data')"

# Switch to non-root user
USER 1001

# Execute start.sh with bash
CMD ["/bin/bash", "/app/start.sh"]