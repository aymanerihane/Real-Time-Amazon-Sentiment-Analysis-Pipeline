#!/usr/bin/env python3
import os
import logging
import pandas as pd
import joblib
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, pandas_udf, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import numpy
from pymongo import MongoClient
from datetime import datetime


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka1:9092,kafka2:9093')
KAFKA_INPUT_TOPIC = os.getenv('KAFKA_INPUT_TOPIC', 'amazon-reviews-raw')
KAFKA_OUTPUT_TOPIC = os.getenv('KAFKA_OUTPUT_TOPIC', 'sentiment-results')
SPARK_MASTER = os.getenv('SPARK_MASTER_URL', 'spark://spark-master:7077')
MODEL_PATH = os.getenv('MODEL_PATH', '/app/resources/sentiment_model_sklearn.pkl')
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://mongodb:27017/')
MONGO_DB = os.getenv('MONGO_DB', 'amazon_reviews')
MONGO_COLLECTION = os.getenv('MONGO_COLLECTION', 'sentiment_results')

# Fix NumPy compatibility issues (add before model loading)
try:
    import numpy
    # For specific numpy version issues
    import sys
    if not hasattr(numpy, '_core'):
        sys.modules['numpy._core'] = numpy
    logger.info(f"NumPy version: {numpy.__version__}")
except Exception as e:
    logger.error(f"Error setting up NumPy compatibility: {str(e)}")

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
        # Try to load the model with safe unpickle settings
        logger.info(f"Attempting to load model from {MODEL_PATH}")
        model = joblib.load(MODEL_PATH, mmap_mode='r')
        logger.info("Model loaded successfully")
        
        # Broadcast the model if spark session is provided
        if spark is not None:
            model = spark.sparkContext.broadcast(model)
            logger.info("Model successfully broadcasted")
            
        return model
    except Exception as e:
        logger.error(f"Error loading model: {str(e)}")
        # Try alternative approach for model loading
        try:
            logger.info("Attempting alternative model loading approach")
            # Using pickle directly with custom unpickler
            import pickle
            import io
            with open(MODEL_PATH, 'rb') as file:
                model = pickle.load(file)
            logger.info("Model loaded with alternative method")
            
            # Broadcast the model if spark session is provided
            if spark is not None:
                model = spark.sparkContext.broadcast(model)
                logger.info("Model successfully broadcasted")
                
            return model
        except Exception as e2:
            logger.error(f"Alternative model loading also failed: {str(e2)}")
            raise

# Define a global variable for the broadcasted model
_model_broadcast = None

# Define pandas UDF for sentiment prediction
@pandas_udf(StringType())
def predict_sentiment(reviews: pd.Series) -> pd.Series:
    """Predict sentiment for each review using the pre-trained model"""
    # Access the broadcasted model
    global _model_broadcast
    
    # Use the broadcasted model value for prediction
    model = _model_broadcast.value if _model_broadcast else load_model()
    
    # For simplicity, we'll use the raw text
    # In production, you'd add proper preprocessing here
    predictions = model.predict(reviews)
    return pd.Series(predictions)

# MongoDB connection
def get_mongo_client():
    """Create and return a MongoDB client"""
    try:
        client = MongoClient(MONGO_URI)
        logger.info("MongoDB connection established")
        return client
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {str(e)}")
        raise

def store_in_mongodb(data):
    """Store the processed data in MongoDB"""
    try:
        client = get_mongo_client()
        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION]
        
        # Add timestamp
        data['stored_at'] = datetime.utcnow()
        
        # Insert the document
        result = collection.insert_one(data)
        logger.info(f"Document stored in MongoDB with ID: {result.inserted_id}")
        
        client.close()
    except Exception as e:
        logger.error(f"Failed to store data in MongoDB: {str(e)}")
        # Don't raise the exception to prevent stream processing from stopping

def main():
    """Main function to process Kafka stream with sentiment analysis"""
    global _model_broadcast
    
    logger.info("Starting sentiment analysis service...")
    
    try:
        # Print Python environment info for debugging
        import sys
        logger.info(f"Python version: {sys.version}")
        
        # Check and report NumPy version
        try:
            import numpy
            logger.info(f"NumPy version: {numpy.__version__}")
        except ImportError:
            logger.warning("NumPy not available - will attempt to install")
            os.system("pip install numpy==1.23.5")  # Install compatible version
        
        # Initialize Spark session
        spark = SparkSession.builder \
            .appName("SimpleSentimentAnalysis") \
            .master(SPARK_MASTER) \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
            .config("spark.python.worker.reuse", "true") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session initialized")
        
        # Load and broadcast the model for efficient distribution
        try:
            _model_broadcast = load_model(spark)
            logger.info("Model broadcasted to Spark cluster")
        except Exception as e:
            logger.error(f"Failed to load model: {str(e)}")
            logger.info("Using fallback sentiment analysis")
            
            # Define a fallback simple sentiment analysis function as pandas UDF
            @pandas_udf(StringType())
            def fallback_sentiment(texts):
                # Very basic sentiment analysis
                results = []
                for text in texts:
                    if not isinstance(text, str):
                        results.append("neutral")
                        continue
                    
                    pos_words = ['good', 'great', 'excellent', 'love', 'like', 'best']
                    neg_words = ['bad', 'terrible', 'worst', 'hate', 'dislike', 'poor']
                    
                    text = text.lower()
                    pos_count = sum(word in text for word in pos_words)
                    neg_count = sum(word in text for word in neg_words)
                    
                    if pos_count > neg_count:
                        results.append("positive")
                    elif neg_count > pos_count:
                        results.append("negative")
                    else:
                        results.append("neutral")
                        
                return pd.Series(results)
            
            # Replace the main predict function with the fallback
            global predict_sentiment
            predict_sentiment = fallback_sentiment
        
        # Create streaming DataFrame from Kafka
        kafka_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", KAFKA_INPUT_TOPIC) \
            .option("startingOffsets", "latest") \
            .load()
        
        # Parse JSON data from Kafka
        parsed_df = kafka_df \
            .selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), get_review_schema()).alias("data")) \
            .select("data.*")
        
        # Apply sentiment prediction using pandas_udf
        result_df = parsed_df \
            .withColumn("sentiment", predict_sentiment(col("reviewText"))) \
            .withColumn("processed_at", current_timestamp().cast(StringType()))
        
        
        # Write results back to Kafka and store in MongoDB
        def process_batch(batch_df, batch_id):
            # Convert to pandas for easier MongoDB insertion
            pandas_df = batch_df.toPandas()
            
            # Store each row in MongoDB
            for _, row in pandas_df.iterrows():
                store_in_mongodb(row.to_dict())
        
        # Write results to both Kafka and MongoDB
        query = result_df \
            .writeStream \
            .foreachBatch(process_batch) \
            .selectExpr(
                "CAST(reviewerID AS STRING) AS key",
                "to_json(struct(*)) AS value"
            ) \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("topic", KAFKA_OUTPUT_TOPIC) \
            .option("checkpointLocation", "/tmp/checkpoint") \
            .outputMode("append") \
            .start()
        
        logger.info(f"Processing stream from {KAFKA_INPUT_TOPIC} to {KAFKA_OUTPUT_TOPIC}")
        query.awaitTermination()
        
    except Exception as e:
        logger.error(f"Error in main process: {str(e)}")
        raise
    finally:
        if 'spark' in locals():
            spark.stop()
            logger.info("Spark session stopped")

if __name__ == "__main__":
    main()