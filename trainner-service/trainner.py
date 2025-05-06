#!/usr/bin/env python3
# Trainer Service with Spark Streaming for Sentiment Analysis
import os
import json
import logging
import time
import nltk
import pickle
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import StructType, StructField, StringType, FloatType, BooleanType, TimestampType
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, StopWordsRemover
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pymongo import MongoClient
from nltk.sentiment.vader import SentimentIntensityAnalyzer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration from environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka1:9092,kafka2:9093')
KAFKA_INPUT_TOPIC = os.getenv('KAFKA_INPUT_TOPIC', 'amazon-reviews-raw')
KAFKA_OUTPUT_TOPIC = os.getenv('KAFKA_OUTPUT_TOPIC', 'sentiment-results')
SPARK_MASTER = os.getenv('SPARK_MASTER', 'spark://spark-master:7077')
MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://root:example@mongodb:27017/amazon_reviews?authSource=admin')

# Path for model saving
MODEL_PATH = "/data/sentiment_model"

# Download NLTK resources
def download_nltk_resources():
    nltk.download('punkt')
    nltk.download('stopwords')
    nltk.download('vader_lexicon')

# Connect to MongoDB
def get_mongodb_client():
    return MongoClient(MONGODB_URI)

# Define the schema for reviews
def get_review_schema():
    return StructType([
        StructField("product_id", StringType(), True),
        StructField("review_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("rating", FloatType(), True),
        StructField("text", StringType(), True),
        StructField("date", StringType(), True),
        StructField("verified", BooleanType(), True),
        StructField("scrape_time", StringType(), True)
    ])

# Create and train sentiment analysis model
def train_sentiment_model(spark):
    """Train or load a sentiment analysis model"""
    # Check if model already exists
    import os.path
    if os.path.exists(MODEL_PATH):
        logger.info(f"Loading existing model from {MODEL_PATH}")
        return PipelineModel.load(MODEL_PATH)
    
    # If no model exists, create a simple one based on ratings
    logger.info("Training new sentiment model")
    
    # Try to load some data from MongoDB to train the model
    db = get_mongodb_client().get_database()
    reviews_collection = db.reviews
    
    # Get some training data
    reviews = list(reviews_collection.find({}, limit=1000))
    
    # If we don't have enough data yet, use a basic VADER sentiment analyzer
    if len(reviews) < 100:
        logger.info("Not enough data for training. Using VADER sentiment analyzer.")
        vader = SentimentIntensityAnalyzer()
        
        # Save the VADER analyzer to use instead of ML model
        with open(f"{MODEL_PATH}_vader.pkl", "wb") as f:
            pickle.dump(vader, f)
        
        return None
    
    # Convert MongoDB data to Spark DataFrame
    review_data = [(
        r.get("product_id", ""), 
        r.get("review_id", ""),
        r.get("title", ""),
        r.get("rating", 0.0),
        r.get("text", ""),
        r.get("date", ""),
        r.get("verified", False),
        r.get("scrape_time", "")
    ) for r in reviews]
    
    df = spark.createDataFrame(review_data, get_review_schema())
    
    # Create label based on rating (rating > 3 is positive)
    df = df.withColumn("label", (col("rating") > 3.0).cast("double"))
    
    # Feature pipeline
    tokenizer = Tokenizer(inputCol="text", outputCol="words")
    remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
    hashingTF = HashingTF(inputCol="filtered_words", outputCol="rawFeatures", numFeatures=10000)
    idf = IDF(inputCol="rawFeatures", outputCol="features")
    
    # Model
    lr = LogisticRegression(maxIter=10, regParam=0.001)
    
    # Create pipeline
    pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, idf, lr])
    
    # Train the model
    model = pipeline.fit(df)
    
    # Save the model
    model.write().overwrite().save(MODEL_PATH)
    
    # Evaluate the model
    predictions = model.transform(df)
    evaluator = BinaryClassificationEvaluator()
    accuracy = evaluator.evaluate(predictions)
    logger.info(f"Model trained with accuracy: {accuracy}")
    
    return model

# Process a single review batch with Spark
def process_reviews_with_spark(spark_session, df):
    """Process reviews with sentiment analysis"""
    # Check if we have a trained model
    import os.path
    if os.path.exists(f"{MODEL_PATH}_vader.pkl"):
        # Use VADER for sentiment analysis
        with open(f"{MODEL_PATH}_vader.pkl", "rb") as f:
            vader = pickle.load(f)
            
        # Define UDF for VADER sentiment analysis
        def get_vader_sentiment(text):
            if not text:
                return 0.0
            scores = vader.polarity_scores(text)
            # Return compound score normalized to 0-1 range
            return (scores['compound'] + 1) / 2
            
        vader_udf = udf(get_vader_sentiment, FloatType())
        
        # Apply sentiment analysis
        result_df = df.withColumn("sentiment_score", vader_udf(col("text")))
        result_df = result_df.withColumn("sentiment", 
                                         (col("sentiment_score") > 0.6).cast("int"))
        
    elif os.path.exists(MODEL_PATH):
        # Use trained ML model
        model = PipelineModel.load(MODEL_PATH)
        result_df = model.transform(df)
        
        # Extract probability of positive sentiment
        result_df = result_df.withColumn("sentiment_score", 
                                        col("probability").getItem(1))
    else:
        # Fall back to rating-based sentiment if no model exists
        result_df = df.withColumn("sentiment_score", 
                                 (col("rating") / 5.0))
        result_df = result_df.withColumn("sentiment", 
                                        (col("rating") > 3.0).cast("int"))
    
    # Return processed DataFrame
    return result_df

# Save results to MongoDB
def save_to_mongodb(reviews_batch):
    """Save processed reviews to MongoDB"""
    if not reviews_batch:
        return
        
    client = get_mongodb_client()
    db = client.get_database()
    reviews_collection = db.reviews
    sentiment_collection = db.sentiment_analysis
    
    # Convert to list of dictionaries for MongoDB insertion
    reviews = reviews_batch.toJSON().collect()
    reviews = [json.loads(review) for review in reviews]
    
    # Insert reviews
    if reviews:
        # Update reviews using upsert
        for review in reviews:
            reviews_collection.update_one(
                {"review_id": review.get("review_id")},
                {"$set": review},
                upsert=True
            )
            
            # Insert sentiment analysis result
            sentiment_data = {
                "review_id": review.get("review_id"),
                "product_id": review.get("product_id"),
                "sentiment_score": review.get("sentiment_score"),
                "sentiment": review.get("sentiment", 1 if review.get("sentiment_score", 0) > 0.6 else 0),
                "processed_at": time.time()
            }
            
            sentiment_collection.update_one(
                {"review_id": review.get("review_id")},
                {"$set": sentiment_data},
                upsert=True
            )
    
    # Close the connection
    client.close()

def main():
    """Main function for the trainer service"""
    logger.info("Trainer Service starting...")
    
    # Download NLTK resources
    download_nltk_resources()
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("AmazonReviewSentimentAnalysis") \
        .master(SPARK_MASTER) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
        .config("spark.mongodb.input.uri", MONGODB_URI) \
        .config("spark.mongodb.output.uri", MONGODB_URI) \
        .getOrCreate()
    
    # Set log level to reduce verbosity
    spark.sparkContext.setLogLevel("WARN")
    
    # Train or load the sentiment model
    sentiment_model = train_sentiment_model(spark)
    
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
    
    # Parse JSON from Kafka
    parsed_df = df \
        .selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")
    
    # Process the stream with sentiment analysis
    def process_batch(batch_df, batch_id):
        """Process each batch of streaming data"""
        if batch_df.count() > 0:
            logger.info(f"Processing batch {batch_id} with {batch_df.count()} reviews")
            
            # Process reviews with sentiment analysis
            processed_df = process_reviews_with_spark(spark, batch_df)
            
            # Save to MongoDB
            save_to_mongodb(processed_df)
            
            # Send results back to Kafka
            processed_df.selectExpr(
                "CAST(review_id AS STRING) AS key",
                "to_json(struct(*)) AS value"
            ).write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
                .option("topic", KAFKA_OUTPUT_TOPIC) \
                .save()
                
            logger.info(f"Completed processing batch {batch_id}")
    
    # Start the streaming query with foreachBatch
    query = parsed_df \
        .writeStream \
        .foreachBatch(process_batch) \
        .outputMode("update") \
        .trigger(processingTime="1 minute") \
        .start()
    
    # Wait for the streaming query to finish
    query.awaitTermination()

if __name__ == "__main__":
    main()