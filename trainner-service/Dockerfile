FROM python:3.10-slim

WORKDIR /app

# Install Java for PySpark
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    wget \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Set Java home
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64
ENV PATH $JAVA_HOME/bin:$PATH

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY . .

# Run the trainer service
CMD ["python", "trainer.py"]