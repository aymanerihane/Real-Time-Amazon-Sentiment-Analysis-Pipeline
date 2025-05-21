#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Real-time sentiment analysis service using Spark Streaming and Kafka
Consumes Amazon reviews from Kafka input topic, predicts sentiment, and publishes results to output topic
"""

import os
import json
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType
from pyspark.ml import PipelineModel
from pymongo import MongoClient

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka1:9092,kafka2:9094")
KAFKA_INPUT_TOPIC = os.getenv("KAFKA_INPUT_TOPIC", "amazon-reviews-raw")
KAFKA_OUTPUT_TOPIC = os.getenv("KAFKA_OUTPUT_TOPIC", "sentiment-results")

# MongoDB configuration
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://root:example@mongodb:27017/amazon_reviews?authSource=admin")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "amazon_reviews")
MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION", "sentiment_results")

# Model path
MODEL_PATH = "/model/sentiment_model"

def create_spark_session():
    """Create and return a Spark Streaming session"""
    return (SparkSession.builder
            .appName("Amazon Reviews Sentiment Analysis Service")
            .config("spark.driver.memory", "2g")
            .config("spark.executor.memory", "2g")
            .config("spark.sql.streaming.checkpointLocation", "/checkpoints")
            .getOrCreate())

def load_model():
    """Load the pre-trained sentiment analysis model"""
    logger.info(f"Loading model from {MODEL_PATH}")
    try:
        model = PipelineModel.load(MODEL_PATH)
        logger.info("Model loaded successfully")
        return model
    except Exception as e:
        logger.error(f"Error loading model: {str(e)}")
        raise

def define_schema():
    """Define the schema for the input JSON data"""
    return StructType([
        StructField("reviewerID", StringType()),
        StructField("asin", StringType()),
        StructField("reviewerName", StringType()),
        StructField("helpful", StringType()),
        StructField("reviewText", StringType()),
        StructField("overall", FloatType()),
        StructField("summary", StringType()),
        StructField("unixReviewTime", IntegerType()),
        StructField("reviewTime", StringType())
    ])

def define_output_schema():
    """Define the schema for the output JSON data"""
    return StructType([
        StructField("reviewerID", StringType()),
        StructField("asin", StringType()),
        StructField("reviewerName", StringType()),
        StructField("reviewText", StringType()),
        StructField("overall", FloatType()),
        StructField("summary", StringType()),
        StructField("reviewTime", StringType()),
        StructField("sentiment", IntegerType()),
        StructField("sentiment_label", StringType()),
        StructField("prediction_time", TimestampType())
    ])

def map_sentiment_label(sentiment):
    """Map numeric sentiment to text label"""
    if sentiment == 0:
        return "Negative"
    elif sentiment == 1:
        return "Neutral"
    else:
        return "Positive"

def store_to_mongodb(batch_df, batch_id):
    """Store the prediction batch in MongoDB"""
    try:
        # Convert batch DataFrame to list of dictionaries for MongoDB insertion
        rows = batch_df.collect()
        documents = []
        
        for row in rows:
            # Convert row to dictionary
            doc = row.asDict()
            documents.append(doc)
        
        if documents:  # Only insert if there are documents
            # Connect to MongoDB
            client = MongoClient(MONGODB_URI)
            db = client[MONGODB_DATABASE]
            collection = db[MONGODB_COLLECTION]
            
            # Insert documents
            result = collection.insert_many(documents)
            logger.info(f"Inserted {len(result.inserted_ids)} documents to MongoDB (batch {batch_id})")
            
            # Close connection
            client.close()
    except Exception as e:
        logger.error(f"Error storing batch {batch_id} to MongoDB: {str(e)}")

def process_stream(spark, model):
    """Process the Kafka stream with the loaded model"""
    logger.info(f"Starting to consume from Kafka topic {KAFKA_INPUT_TOPIC}")
    
    # Define the schema for the input JSON
    schema = define_schema()
    
    # Read from Kafka
    df_stream = (spark
                 .readStream
                 .format("kafka")
                 .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
                 .option("subscribe", KAFKA_INPUT_TOPIC)
                 .option("startingOffsets", "latest")
                 .load())
    
    # Parse the JSON from Kafka
    parsed_df = df_stream.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    # Filter out records with missing review text
    filtered_df = parsed_df.filter(col("reviewText").isNotNull())
    
    # Apply the model to make predictions
    predictions = model.transform(filtered_df)
    
    # Add sentiment label and prediction timestamp
    sentiment_label_udf = spark.udf.register("sentiment_label_udf", map_sentiment_label, StringType())
    
    result_df = predictions.select(
        col("reviewerID"),
        col("asin"),
        col("reviewerName"),
        col("reviewText"),
        col("overall"),
        col("summary"),
        col("reviewTime"),
        col("prediction").alias("sentiment"),
        sentiment_label_udf(col("prediction")).alias("sentiment_label"),
        current_timestamp().alias("prediction_time")
    )
    
    # Write the results back to Kafka
    query = (result_df
             .select(to_json(struct("*")).alias("value"))
             .writeStream
             .format("kafka")
             .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
             .option("topic", KAFKA_OUTPUT_TOPIC)
             .outputMode("append")
             .start())
    
    # Also write to console for debugging in development
    console_query = (result_df
                    .writeStream
                    .format("console")
                    .option("truncate", False)
                    .outputMode("append")
                    .start())
    
    # Write to MongoDB
    mongodb_query = (result_df
                    .writeStream
                    .foreachBatch(store_to_mongodb)
                    .outputMode("append")
                    .start())
    
    # Wait for the termination of all streams
    query.awaitTermination()
    console_query.awaitTermination()
    mongodb_query.awaitTermination()

def main():
    """Main function to start the sentiment analysis service"""
    logger.info("Starting Amazon Reviews sentiment analysis service")
    
    # Create Spark session
    spark = create_spark_session()
    
    # Load pre-trained model
    model = load_model()
    
    # Process the streaming data
    process_stream(spark, model)

if __name__ == "__main__":
    main()