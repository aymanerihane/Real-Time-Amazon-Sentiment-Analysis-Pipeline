#!/usr/bin/env python3
import os
import json
import logging
import pickle
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, pandas_udf, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, FloatType, StructType, StructField
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
import re
import pandas as pd
import numpy as np
import joblib

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka1:9092,kafka2:9093')
KAFKA_INPUT_TOPIC = os.getenv('KAFKA_INPUT_TOPIC', 'amazon-reviews-raw')
KAFKA_OUTPUT_TOPIC = os.getenv('KAFKA_OUTPUT_TOPIC', 'sentiment-results')
SPARK_MASTER = os.getenv('SPARK_MASTER_URL', 'spark://spark-master:7077')
MODEL_PATH = os.getenv('MODEL_PATH', '/app/resources/sentiment_model_sklearn.pkl')

def smooth_text(text):
    """Apply text smoothing techniques"""
    if not isinstance(text, str):
        return ""
    
    # Convert to lowercase
    text = text.lower()
    
    # Remove URLs
    text = re.sub(r'http\S+|www\S+|https\S+', '', text, flags=re.MULTILINE)
    
    # Remove special characters and digits but keep important punctuation
    text = re.sub(r'[^a-zA-Z\s.,!?]', '', text)
    
    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text

def preprocess_text(text):
    """Preprocess text data with lemmatization"""
    if not isinstance(text, str):
        return ""
    
    # Apply text smoothing
    text = smooth_text(text)
    
    # Tokenize
    tokens = word_tokenize(text)
    
    # Initialize lemmatizer
    lemmatizer = WordNetLemmatizer()
    
    # Remove stopwords and lemmatize
    stop_words = set(stopwords.words('english'))
    tokens = [lemmatizer.lemmatize(token) for token in tokens if token not in stop_words]
    
    # Remove short tokens (length < 2)
    tokens = [token for token in tokens if len(token) > 1]
    
    return ' '.join(tokens)

def get_review_schema():
    """Define the schema for reviews"""
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

def get_output_schema():
    """Define the schema for output"""
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
        StructField("sentiment", StringType(), True),
        StructField("processed_at", StringType(), True)
    ])

def load_model():
    """Load the trained scikit-learn model"""
    try:
        model = joblib.load(MODEL_PATH)
        logger.info("Model loaded successfully")
        return model
    except Exception as e:
        logger.error(f"Error loading model: {str(e)}")
        raise

def process_reviews_with_spark(spark_session, df, model):
    """Process reviews with sentiment analysis using scikit-learn model"""
    if model is None:
        logger.error("No model available for sentiment analysis")
        return df
    
    # Get the original schema
    original_schema = df.schema
    
    # Convert to pandas for processing
    pandas_df = df.toPandas()
    
    try:
        # Process reviews in pandas
        processed_texts = pandas_df['reviewText'].apply(preprocess_text)
        
        # Ensure model is loaded in the current process
        if not hasattr(model, 'predict'):
            model = load_model()
        
        predictions = model.predict(processed_texts)
        
        # Add predictions back to pandas DataFrame
        pandas_df['sentiment'] = predictions
        pandas_df['processed_at'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Convert back to Spark DataFrame using the original schema plus new columns
        result_df = spark_session.createDataFrame(
            pandas_df,
            schema=original_schema.add("sentiment", StringType(), True)
                                .add("processed_at", StringType(), True)
        )
        
        return result_df
    except Exception as e:
        logger.error(f"Error in process_reviews_with_spark: {str(e)}")
        # Return original DataFrame with neutral sentiment in case of error
        return df.withColumn("sentiment", lit("neutral")) \
                .withColumn("processed_at", current_timestamp().cast(StringType()))

def main():
    """Main function for the ML service"""
    logger.info("ML Service starting...")
    
    try:
        # Load the scikit-learn model to verify it works
        model = load_model()
        logger.info("Model loaded successfully")
        
        # Initialize Spark session with additional configurations
        spark = SparkSession.builder \
            .appName("AmazonReviewSentimentAnalysis") \
            .master(SPARK_MASTER) \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
            .config("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8") \
            .config("spark.executor.extraJavaOptions", "-Dfile.encoding=UTF-8") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()
        
        # Set log level to reduce verbosity
        spark.sparkContext.setLogLevel("WARN")
        
        logger.info("Spark session initialized successfully")
        
        # Define the schema for Kafka messages
        schema = get_review_schema()
        
        # Create streaming DataFrame from Kafka
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", KAFKA_INPUT_TOPIC) \
            .option("startingOffsets", "latest") \
            .load()
        
        logger.info("Kafka stream initialized successfully")
        
        # Parse JSON from Kafka
        parsed_df = df \
            .selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), schema).alias("data")) \
            .select("data.*")
        
        # Process the stream with sentiment analysis
        def process_batch(batch_df, batch_id):
            """Process each batch of streaming data"""
            try:
                if batch_df.count() > 0:
                    logger.info(f"Processing batch {batch_id} with {batch_df.count()} reviews")
                    
                    # Process reviews with sentiment analysis
                    processed_df = process_reviews_with_spark(spark, batch_df, model)
                    
                    # Send results back to Kafka with all fields
                    processed_df.selectExpr(
                        "CAST(reviewerID AS STRING) AS key",
                        "to_json(struct(reviewerID, asin, title, reviewText, overall, reviewer_name, date, helpful_votes, total_votes, scrape_time, sentiment, processed_at)) AS value"
                    ).write \
                        .format("kafka") \
                        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
                        .option("topic", KAFKA_OUTPUT_TOPIC) \
                        .save()
                        
                    logger.info(f"Completed processing batch {batch_id}")
            except Exception as e:
                logger.error(f"Error processing batch {batch_id}: {str(e)}")
        
        # Start the streaming query with foreachBatch
        query = parsed_df \
            .writeStream \
            .foreachBatch(process_batch) \
            .outputMode("update") \
            .trigger(processingTime="1 minute") \
            .start()
        
        logger.info("Streaming query started successfully")
        
        # Wait for the streaming query to finish
        query.awaitTermination()
        
    except Exception as e:
        logger.error(f"Error in main process: {str(e)}")
        raise
    finally:
        # Clean up Spark session
        if 'spark' in locals():
            spark.stop()
            logger.info("Spark session stopped")

if __name__ == "__main__":
    main()