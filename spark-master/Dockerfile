FROM bitnami/spark:3.1.3

USER root

# Install Python dependencies
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Set up directories
RUN mkdir -p /model /checkpoints

# Copy Python files
COPY train_model.py /train_model.py
COPY ml_service.py /ml_service.py
COPY data/Data.json /Data.json
COPY data/Data.json /opt/spark-data/Data.json

# Make Python files executable
RUN chmod +x /train_model.py /ml_service.py

# Fix permissions for the model directory
RUN chown -R 1001:1001 /model /checkpoints

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONIOENCODING=UTF-8

USER 1001
