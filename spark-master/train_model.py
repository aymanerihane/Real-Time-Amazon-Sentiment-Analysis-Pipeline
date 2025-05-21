#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Train sentiment analysis model on Amazon reviews data
"""

import os
import sys
import json
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, udf
from pyspark.sql.types import StringType, IntegerType
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer, IDF
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, NaiveBayes
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Define model save paths
MODEL_DIR = "/model"
VECTORIZER_PATH = f"{MODEL_DIR}/vectorizer"
IDF_MODEL_PATH = f"{MODEL_DIR}/idf_model"
FINAL_MODEL_PATH = f"{MODEL_DIR}/sentiment_model"

def create_spark_session():
    """Create and return a Spark session"""
    return (SparkSession.builder
            .appName("Amazon Reviews Sentiment Analysis")
            .config("spark.driver.memory", "2g")
            .config("spark.executor.memory", "2g")
            .getOrCreate())

def load_data(spark, data_path):
    """Load the JSON data file into a Spark DataFrame"""
    logger.info(f"Loading data from {data_path}")
    try:
        # Read the JSON file
        df = spark.read.json(data_path)
        logger.info(f"Data loaded successfully with {df.count()} records")
        
        # Show the schema to verify
        df.printSchema()
        
        return df
    except Exception as e:
        logger.error(f"Error loading data: {str(e)}")
        sys.exit(1)

def preprocess_data(df):
    """Preprocess the data for training"""
    logger.info("Preprocessing data")
    
    # Define sentiment labels based on overall rating
    df = df.withColumn("sentiment", 
                      when(col("overall") < 3, 0)  # Negative
                     .when(col("overall") == 3, 1)  # Neutral
                     .otherwise(2))  # Positive
    
    # Filter out rows with missing review text
    df = df.filter(col("reviewText").isNotNull())
    
    # Select relevant columns
    df = df.select("reviewText", "sentiment")
    
    # Display class distribution
    class_counts = df.groupBy("sentiment").count().orderBy("sentiment")
    class_counts.show()
    
    return df

def create_pipeline():
    """Create ML pipeline for text processing and model training"""
    logger.info("Creating ML pipeline")
    
    # Text preprocessing
    tokenizer = RegexTokenizer(inputCol="reviewText", outputCol="words", pattern="\\W")
    remover = StopWordsRemover(inputCol="words", outputCol="filtered")
    vectorizer = CountVectorizer(inputCol="filtered", outputCol="raw_features", minDF=5.0)
    idf = IDF(inputCol="raw_features", outputCol="features")
    
    # Models to try
    lr = LogisticRegression(maxIter=20, regParam=0.3, elasticNetParam=0, labelCol="sentiment", featuresCol="features")
    rf = RandomForestClassifier(numTrees=20, maxDepth=5, seed=42, labelCol="sentiment", featuresCol="features")
    nb = NaiveBayes(smoothing=1.0, labelCol="sentiment", featuresCol="features")
    
    # Create pipelines for each model
    lr_pipeline = Pipeline(stages=[tokenizer, remover, vectorizer, idf, lr])
    rf_pipeline = Pipeline(stages=[tokenizer, remover, vectorizer, idf, rf])
    nb_pipeline = Pipeline(stages=[tokenizer, remover, vectorizer, idf, nb])
    
    return {
        "LogisticRegression": lr_pipeline,
        "RandomForest": rf_pipeline,
        "NaiveBayes": nb_pipeline
    }

def train_and_evaluate_models(df):
    """Train and evaluate multiple models to find the best one"""
    logger.info("Training and evaluating models")
    
    # Split data into train (80%), validation (10%), and test (10%)
    train_data, temp_data = df.randomSplit([0.8, 0.2], seed=42)
    val_data, test_data = temp_data.randomSplit([0.5, 0.5], seed=42)
    
    logger.info(f"Train size: {train_data.count()}, Validation size: {val_data.count()}, Test size: {test_data.count()}")
    
    # Create pipelines
    pipelines = create_pipeline()
    
    # Train and evaluate each model
    results = {}
    best_model = None
    best_score = 0.0
    
    for name, pipeline in pipelines.items():
        logger.info(f"Training {name} model")
        model = pipeline.fit(train_data)
        
        # Evaluate on validation set
        predictions = model.transform(val_data)
        evaluator = MulticlassClassificationEvaluator(labelCol="sentiment", predictionCol="prediction", metricName="accuracy")
        accuracy = evaluator.evaluate(predictions)
        
        logger.info(f"{name} - Validation Accuracy: {accuracy:.4f}")
        results[name] = (model, accuracy)
        
        # Keep track of best model
        if accuracy > best_score:
            best_score = accuracy
            best_model = (name, model)
    
    # Final evaluation on test set
    best_name, best_model_fit = best_model
    logger.info(f"Best model: {best_name} with validation accuracy: {best_score:.4f}")
    
    test_predictions = best_model_fit.transform(test_data)
    test_accuracy = evaluator.evaluate(test_predictions)
    logger.info(f"Final test accuracy: {test_accuracy:.4f}")
    
    return best_model_fit

def save_model(model):
    """Save the trained model"""
    logger.info(f"Saving model to {FINAL_MODEL_PATH}")
    
    # Create directory if it doesn't exist
    # os.makedirs(MODEL_DIR, exist_ok=True)
    
    # Save the model
    try:
        model.write().overwrite().save(FINAL_MODEL_PATH)
        logger.info("Model saved successfully")
    except Exception as e:
        logger.error(f"Error saving model: {str(e)}")
        sys.exit(1)

def main():
    """Main function to train the sentiment analysis model"""
    logger.info("Starting model training")
    
    # Create Spark session
    spark = create_spark_session()
    
    # Load data
    data_path = "/opt/spark-data/Data.json"
    df = load_data(spark, data_path)
    
    # Preprocess data
    df_processed = preprocess_data(df)
    
    # Train models and select the best one
    best_model = train_and_evaluate_models(df_processed)
    
    # Save the best model
    save_model(best_model)
    
    logger.info("Model training completed successfully")
    spark.stop()

if __name__ == "__main__":
    main()