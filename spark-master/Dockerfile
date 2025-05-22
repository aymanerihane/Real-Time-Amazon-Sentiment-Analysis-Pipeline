FROM bitnami/spark:3.1.3

USER root

# Install Python dependencies
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Set up directories
RUN mkdir -p /model /checkpoints
RUN mkdir -p /nltk/data

# Copy Python files
COPY train_model.py /train_model.py
COPY ml_service.py /ml_service.py
COPY data/Data.json /Data.json
COPY data/Data.json /opt/spark-data/Data.json

# Make Python files executable
RUN chmod +x /train_model.py /ml_service.py

# Fix permissions for directories
RUN chown -R 1001:1001 /model /checkpoints /nltk
RUN chmod -R 775 /nltk

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONIOENCODING=UTF-8
ENV NLTK_DATA=/nltk/data

USER 1001
