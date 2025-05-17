FROM bitnami/spark:3.1.3

# Install pip and required packages
USER root

# Install system dependencies
RUN apt-get update && \
    apt-get install -y wget netcat-traditional && \
    rm -rf /var/lib/apt/lists/*

# Copy your requirements file
COPY start.sh /start.sh
COPY ml_service.py /ml_service.py
COPY train_model.py /train_model.py
COPY Data.json /Data.json
COPY resources/sentiment_model_sklearn.pkl /sentiment_model_sklearn.pkl
COPY requirements.txt /opt/bitnami/spark/requirements.txt

# Install Python dependencies in the correct order
RUN pip install --no-cache-dir -r /opt/bitnami/spark/requirements.txt 
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# Set up NLTK data with retries and error handling
# RUN mkdir -p /opt/nltk_data && \
#     python3 -c "import nltk; \
#     nltk.data.path.append('/opt/nltk_data'); \
#     nltk.download('punkt', download_dir='/opt/nltk_data'); \
#     nltk.download('stopwords', download_dir='/opt/nltk_data'); \
#     nltk.download('wordnet', download_dir='/opt/nltk_data')"

# Set environment variables

USER 1001

CMD ["/start.sh"]