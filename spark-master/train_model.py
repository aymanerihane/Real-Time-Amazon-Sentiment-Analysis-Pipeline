
import json
import pandas as pd
import numpy as np
import re
import logging
import os
import sys
from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF, StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.sql.functions import udf, col, when, expr, regexp_replace, lower, concat_ws
from pyspark.sql.types import StringType, ArrayType, DoubleType
import nltk
from nltk.stem import WordNetLemmatizer

# Download NLTK resources
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('wordnet')

# import json
# import pandas as pd
# import numpy as np
# from sklearn.model_selection import train_test_split
# from sklearn.feature_extraction.text import TfidfVectorizer
# from sklearn.pipeline import Pipeline
# from sklearn.linear_model import LogisticRegression
# from sklearn.ensemble import RandomForestClassifier
# from sklearn.svm import SVC
# from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
# from imblearn.over_sampling import SMOTE
# from imblearn.pipeline import Pipeline as ImbPipeline
# import pickle
# import nltk
# from nltk.corpus import stopwords
# from nltk.tokenize import word_tokenize
# from nltk.stem import WordNetLemmatizer
# import re
# import logging
# import os
# import sys


# Define paths
DATA_DIR = '/app'
RESOURCES_DIR = '/app/resources'
RESULTS_DIR = '/app/results'
DATA_FILE = os.path.join(DATA_DIR, 'Data.json')
MODEL_DIR = os.path.join(RESOURCES_DIR, 'sentiment_model_mllib')


# # Define paths
# DATA_DIR = '/app/data'
# RESOURCES_DIR = '/app/resources'
# RESULTS_DIR = '/app/results'
# DATA_FILE = os.path.join(DATA_DIR, 'Data.json')
# MODEL_FILE = os.path.join(RESOURCES_DIR, 'sentiment_model_sklearn.pkl')


# Initialize lemmatizer
lemmatizer = WordNetLemmatizer()

def create_spark_session():
    """Create and return a Spark session"""
    return SparkSession.builder \
        .appName("SentimentAnalysis") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()

def load_data(spark, file_path):
    """Load the data using Spark"""
    logger.info(f"Loading data from JSON file: {file_path}")

    
#     if not os.path.exists(file_path):
#         logger.error(f"Data file not found at: {file_path}")
#         sys.exit(1)
    
    try:
        # Read JSON file
        df = spark.read.json(file_path, multiLine=False)
        
        # Keep only relevant columns
        df = df.select("reviewerID", "asin", "reviewText", "overall", "summary")
        
        # Handle missing values
        df = df.na.fill("", ["reviewText", "summary"])
        
        # Combine review text and summary for better context
        df = df.withColumn("combined_text", 
                          concat_ws(" ", col("summary"), col("reviewText")))
        
        # Create sentiment labels (rating > 3 is positive)
        df = df.withColumn("sentiment", 
                          when(col("overall") > 3, "positive")
                          .when(col("overall") < 3, "negative")
                          .otherwise("neutral"))
        
        # Log data distribution
        logger.info(f"Total reviews: {df.count()}")
        logger.info(f"Positive reviews: {df.filter(col('sentiment') == 'positive').count()}")
        logger.info(f"Negative reviews: {df.filter(col('sentiment') == 'negative').count()}")
        logger.info(f"Neutral reviews: {df.filter(col('sentiment') == 'neutral').count()}")

#         return df
    
#     except Exception as e:
#         logger.error(f"Error loading data: {str(e)}")
#         sys.exit(1)

def preprocess_text_udf():
    """Create UDF for text preprocessing"""
    def lemmatize_text(tokens):
        """Lemmatize tokens"""
        if not tokens:
            return []
        return [lemmatizer.lemmatize(token) for token in tokens if len(token) > 1]
    
    def preprocess(text):
        """Preprocess text data"""
        if not text:
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
    
    return udf(preprocess, StringType())

def create_pipeline(spark):
    """Create ML pipeline for text processing and classification"""
    # Create preprocessing UDF
    preprocessor = preprocess_text_udf()
    
    # Set up pipeline stages
    stages = [
        # Preprocess text
        Tokenizer(inputCol="combined_text", outputCol="tokens"),
        
        # Remove stopwords
        StopWordsRemover(inputCol="tokens", outputCol="filtered_tokens"),
        
        # Create TF features
        HashingTF(inputCol="filtered_tokens", outputCol="raw_features", numFeatures=5000),
        
        # Apply IDF
        IDF(inputCol="raw_features", outputCol="features", minDocFreq=5),
        
        # Index labels
        StringIndexer(inputCol="sentiment", outputCol="label", handleInvalid="keep")
    ]
    
    return stages

def train_and_evaluate_models(spark, data):
    """Train and evaluate models using MLlib"""
    # Split data
    train_data, test_data = data.randomSplit([0.8, 0.2], seed=42)
    
    # Handle class imbalance
    # Note: Spark doesn't have built-in SMOTE, so we'll use class weights and sampling
    # Get class counts
    class_counts = data.groupBy("sentiment").count().collect()
    
    # Calculate class weights for balancing
    total = sum(row["count"] for row in class_counts)
    class_weights = {row["sentiment"]: total / row["count"] / len(class_counts) for row in class_counts}
    
    logger.info(f"Class weights: {class_weights}")
    
    # Create preprocessing pipeline
    preprocessing_stages = create_pipeline(spark)
    preprocessing_pipeline = Pipeline(stages=preprocessing_stages)
    
    # Fit preprocessing pipeline on training data
    preprocessing_model = preprocessing_pipeline.fit(train_data)
    train_processed = preprocessing_model.transform(train_data)
    test_processed = preprocessing_model.transform(test_data)
    
    # Define models
    models = {
        'LogisticRegression': LogisticRegression(
            maxIter=100,
            regParam=0.1,
            elasticNetParam=0.8,
            featuresCol="features",
            labelCol="label"
        ),
        'RandomForest': RandomForestClassifier(
            numTrees=100,
            maxDepth=10,
            featuresCol="features",
            labelCol="label"
        )
    }
    
#     best_model = None
#     best_accuracy = 0
#     results = {}
    
    # Train and evaluate each model
    for name, model in models.items():
        logger.info(f"\nTraining {name}...")
        
        # Train the model
        model_fitted = model.fit(train_processed)
        
        # Make predictions
        predictions = model_fitted.transform(test_processed)
        
        # Evaluate model
        evaluator = MulticlassClassificationEvaluator(
            labelCol="label", 
            predictionCol="prediction", 
            metricName="accuracy"
        )
        accuracy = evaluator.evaluate(predictions)
        
        # Log results
        logger.info(f"{name} Accuracy: {accuracy:.4f}")
        
        # Store results
        results[name] = {
            'model': model_fitted,
            'pipeline': preprocessing_model,
            'accuracy': accuracy
        }
        
        # Update best model
        if accuracy > best_accuracy:
            best_accuracy = accuracy
            best_model = {
                'model': model_fitted,
                'pipeline': preprocessing_model,
                'name': name
            }
    
    # Save detailed results
    with open(os.path.join(RESULTS_DIR, 'training_results.txt'), 'w') as f:
        f.write(f"Best Model: {best_model['name']}\n")
        f.write(f"Best Accuracy: {best_accuracy:.4f}\n\n")
        for name, result in results.items():
            f.write(f"Model: {name}\n")
            f.write(f"Accuracy: {result['accuracy']:.4f}\n")
            f.write("\n" + "="*50 + "\n")

    
#     return best_model, results


def save_model(model_info, model_dir):
    """Save the trained model"""
    try:
        # Create directory if it doesn't exist
        os.makedirs(model_dir, exist_ok=True)
        
        # Save pipeline model
        pipeline_path = os.path.join(model_dir, "pipeline")
        model_info['pipeline'].write().overwrite().save(pipeline_path)
        
        # Save classification model
        model_path = os.path.join(model_dir, "model")
        model_info['model'].write().overwrite().save(model_path)
        
        # Save model name
        with open(os.path.join(model_dir, "model_info.txt"), "w") as f:
            f.write(f"Model: {model_info['name']}\n")
        
        logger.info(f"Model saved to {model_dir}")
        
    except Exception as e:
        logger.error(f"Error saving model: {str(e)}")
        sys.exit(1)

def main():
    try:
        # Create Spark session
        spark = create_spark_session()
        
        # Load and preprocess data
        df = load_data(spark, DATA_FILE)
        
        # Train and evaluate models
        best_model, results = train_and_evaluate_models(spark, df)
        
        # Save the best model
        save_model(best_model, MODEL_DIR)
        
        logger.info("Training completed successfully!")
        logger.info(f"Model saved to {MODEL_DIR}")
        logger.info(f"Results saved to {RESULTS_DIR}/")
        
        # Stop Spark session
        spark.stop()
        
        # Exit with success
        sys.exit(0)

        
#     except Exception as e:
#         logger.error(f"Error in main process: {str(e)}")
#         sys.exit(1)

# if __name__ == "__main__":
#     main() 
