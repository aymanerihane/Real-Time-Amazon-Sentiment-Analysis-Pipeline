#!/usr/bin/env python3
import os
import logging
import pandas as pd
import joblib
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, pandas_udf, current_timestamp, when, lit, expr
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import numpy
from pymongo import MongoClient
from datetime import datetime
import uuid
import nltk
import re
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer

# Initialize NLTK
nltk.data.path.append('/opt/nltk_data')
try:
    nltk.download('punkt', download_dir='/opt/nltk_data', quiet=True)
    nltk.download('stopwords', download_dir='/opt/nltk_data', quiet=True)
    nltk.download('wordnet', download_dir='/opt/nltk_data', quiet=True)
except Exception as e:
    logging.error(f"Error initializing NLTK: {str(e)}")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka1:9092,kafka2:9094')
KAFKA_INPUT_TOPIC = os.getenv('KAFKA_INPUT_TOPIC', 'amazon-reviews-raw')
KAFKA_OUTPUT_TOPIC = os.getenv('KAFKA_OUTPUT_TOPIC', 'sentiment-results')
SPARK_MASTER = os.getenv('SPARK_MASTER_URL', 'spark://spark-master:7077')
MODEL_PATH = os.getenv('MODEL_PATH', '/sentiment_model_sklearn.pkl')
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://root:example@mongodb:27017/amazon_reviews?authSource=admin')
MONGO_DB = os.getenv('MONGO_DB', 'amazon_reviews')
MONGO_COLLECTION = os.getenv('MONGO_COLLECTION', 'sentiment_results')

# Text preprocessing functions as pandas UDF
@pandas_udf(StringType())
def preprocess_text(texts: pd.Series) -> pd.Series:
    """Preprocess text data with lemmatization using pandas vectorization"""
    results = []
    
    # Initialize lemmatizer and stop words once
    lemmatizer = WordNetLemmatizer()
    stop_words = set(stopwords.words('english'))
    
    # Process each text in the Series
    for text in texts:
        if not isinstance(text, str) or not text:
            results.append("")
            continue
        
        # Convert to lowercase
        text = text.lower()
        
        # Remove URLs
        text = re.sub(r'http\S+|www\S+|https\S+', '', text, flags=re.MULTILINE)
        
        # Remove special characters and digits but keep important punctuation
        text = re.sub(r'[^a-zA-Z\s.,!?]', '', text)
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        
        try:
            # Tokenize
            tokens = word_tokenize(text)
            
            # Remove stopwords and lemmatize
            tokens = [lemmatizer.lemmatize(token) for token in tokens if token not in stop_words and len(token) > 1]
            
            results.append(' '.join(tokens))
        except Exception as e:
            logging.error(f"Error preprocessing text: {str(e)}")
            results.append("")
    
    return pd.Series(results)

def get_review_schema():
    """Define the schema for incoming review data"""
    return StructType([
        StructField("reviewerID", StringType(), True),
        StructField("asin", StringType(), True),
        StructField("title", StringType(), True),
        StructField("reviewText", StringType(), True),
        StructField("overall", FloatType(), True),
        StructField("reviewer_name", StringType(), True),
        StructField("date", StringType(), True),
        StructField("helpful_votes", StringType(), True),
        StructField("total_votes", StringType(), True),
        StructField("scrape_time", StringType(), True),
    ])

def load_model(spark=None):
    """Load the pre-trained sentiment analysis model and broadcast it"""
    try:
        logger.info(f"Loading model from {MODEL_PATH}")
        model = joblib.load(MODEL_PATH, mmap_mode='r')
        if spark is not None:
            model = spark.sparkContext.broadcast(model)            
        return model
    except Exception as e:
        logger.error(f"Error loading model: {str(e)}")
        raise

def store_in_mongodb(data):
    """Store the processed data in MongoDB"""
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION]
        
        # Add timestamp and ensure all fields are present
        data['stored_at'] = datetime.utcnow()
        data['id'] = str(uuid.uuid4())
        result = collection.insert_one(data)
        client.close()
    except Exception as e:
        logger.error(f"Failed to store data in MongoDB: {str(e)}")

# Define pandas UDF for sentiment prediction
@pandas_udf(StringType())
def predict_sentiment(reviews: pd.Series) -> pd.Series:
    """Predict sentiment for each review using the pre-trained model"""
    try:
        model = model_broadcast.value
        reviews = reviews.fillna("").astype(str)
        predictions = model.predict(reviews)
        return pd.Series(predictions)
    except Exception as e:
        logger.error(f"Prediction error in batch: {str(e)}")
        return pd.Series([""] * len(reviews))

def main():
    """Main function to process Kafka stream with sentiment analysis"""
    global model_broadcast
    
    logger.info("Starting sentiment analysis service...")
    
    try:
        # Initialize Spark session
        spark = SparkSession.builder \
            .appName("ReviewAnalysis") \
            .master(SPARK_MASTER) \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .config("spark.driver.extraClassPath", "/opt/bitnami/spark/jars/*") \
            .config("spark.executor.extraClassPath", "/opt/bitnami/spark/jars/*") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
            .config("spark.pyspark.python", "/opt/bitnami/python/bin/python3") \
            .config("spark.pyspark.driver.python", "/opt/bitnami/python/bin/python3") \
            .config("spark.executorEnv.PYTHONPATH", "/app:$PYTHONPATH") \
            .config("spark.yarn.appMasterEnv.PYSPARK_PYTHON", "/opt/bitnami/python/bin/python3") \
            .config("spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON", "/opt/bitnami/python/bin/python3") \
            .getOrCreate()
        
        # Verify pandas is available in the driver
        logger.info(f"Pandas version on driver: {pd.__version__}")
        
        # Manually distribute pandas to all workers
        sc = spark.sparkContext
        try:
            # Try to import pandas on each worker
            import_cmd = "import pandas; print('Pandas version:', pandas.__version__)"
            result = sc.parallelize([1]).map(lambda x: exec(import_cmd)).collect()
            logger.info("Successfully verified pandas is available on workers")
        except Exception as e:
            logger.warning(f"Could not verify pandas on workers: {str(e)}")
            # Fallback to installing pandas on workers if not available
            try:
                sc.addPyFile("/opt/bitnami/spark/sbin/spark-worker-setup.sh")
                logger.info("Added worker setup script to distribution")
            except Exception as setup_err:
                logger.error(f"Failed to distribute setup script: {str(setup_err)}")
        
        spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session initialized")
        
        # Load and broadcast the model
        model_broadcast = load_model(spark)
        
        # Create streaming DataFrame from Kafka
        kafka_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", KAFKA_INPUT_TOPIC) \
            .option("failOnDataLoss", "false") \
            .option("startingOffsets", "latest") \
            .load()
        
        # Parse JSON data and process
        parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), get_review_schema()).alias("data")) \
            .select("data.*") \
            .withColumn("processing_time", current_timestamp())
            
        # Apply pandas UDF for text preprocessing
        processed_df = parsed_df \
            .withColumn("processed_text", preprocess_text(col("reviewText"))) \
            .filter(col("reviewText").isNotNull() & (col("reviewText") != ""))
        
        # Apply sentiment prediction
        result_df = processed_df \
            .withColumn(
                "sentiment",
                when((col("processed_text") != ""), predict_sentiment(col("processed_text")))
                .otherwise(lit(""))
            ) \
            .withColumn("id", expr("uuid()")) \
            .select(
                "id", "reviewerID", "asin", "reviewText", "processed_text",
                "overall", "reviewer_name", "date", "sentiment", "processing_time"
            )
        
        # Add debug stream to console
        debug_query = result_df.writeStream \
            .format("console") \
            .outputMode("append") \
            .option("truncate", "false") \
            .trigger(processingTime='5 seconds') \
            .start()
        
        # Process batches and store in MongoDB
        def process_batch(batch_df, batch_id):
            try:
                pandas_df = batch_df.toPandas()
                for _, row in pandas_df.iterrows():
                    store_in_mongodb(row.to_dict())
            except Exception as e:
                logger.error(f"Error processing batch {batch_id}: {str(e)}")
        
        # Write results to Kafka and MongoDB
        logger.info(f"Processing stream from {KAFKA_INPUT_TOPIC} to {KAFKA_OUTPUT_TOPIC}")
        
        query = result_df \
            .selectExpr(
                "CAST(reviewerID AS STRING) AS key",
                "to_json(struct(*)) AS value"
            ) \
            .writeStream \
            .foreachBatch(process_batch) \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("topic", KAFKA_OUTPUT_TOPIC) \
            .option("checkpointLocation", "/tmp/checkpoint") \
            .trigger(processingTime='5 seconds') \
            .outputMode("append") \
            .start()
        
        query.awaitTermination()
    except Exception as e:
        logger.error(f"Error in main process: {str(e)}")
        raise
    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    main()