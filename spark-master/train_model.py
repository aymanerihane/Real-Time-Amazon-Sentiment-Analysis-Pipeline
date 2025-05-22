#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Train sentiment analysis model on Amazon reviews data
"""

import os
import sys
import json
import logging
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import re
import string
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, udf, count, lit, length, lower, regexp_replace, concat
from pyspark.sql.types import StringType, IntegerType, FloatType, ArrayType
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer, IDF, HashingTF, NGram
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, NaiveBayes, GBTClassifier
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.mllib.evaluation import MulticlassMetrics
from sklearn.metrics import confusion_matrix, classification_report
import pandas as pd
from textblob import TextBlob

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Define model save paths
MODEL_DIR = "/model"
VECTORIZER_PATH = f"{MODEL_DIR}/vectorizer"
IDF_MODEL_PATH = f"{MODEL_DIR}/idf_model"
FINAL_MODEL_PATH = f"{MODEL_DIR}/sentiment_model"
CONFUSION_MATRIX_PATH = f"{MODEL_DIR}/confusion_matrix.png"
MODEL_STATS_PATH = f"{MODEL_DIR}/model_statistics.txt"
CONFUSION_REPORT_PATH = f"{MODEL_DIR}/confusion_metrics.txt"

# Ensure directories exist
os.makedirs(MODEL_DIR, exist_ok=True)

# Custom stop words list from the notebook
STOP_WORDS = ['yourselves', 'between', 'whom', 'itself', 'is', "she's", 'up', 'herself', 'here', 'your', 'each', 
             'we', 'he', 'my', "you've", 'having', 'in', 'both', 'for', 'themselves', 'are', 'them', 'other',
             'and', 'an', 'during', 'their', 'can', 'yourself', 'she', 'until', 'so', 'these', 'ours', 'above', 
             'what', 'while', 'have', 're', 'more', 'only', "needn't", 'when', 'just', 'that', 'were', "don't", 
             'very', 'should', 'any', 'y', 'isn', 'who',  'a', 'they', 'to', 'too', "should've", 'has', 'before',
             'into', 'yours', "it's", 'do', 'against', 'on',  'now', 'her', 've', 'd', 'by', 'am', 'from', 
             'about', 'further', "that'll", "you'd", 'you', 'as', 'how', 'been', 'the', 'or', 'doing', 'such',
             'his', 'himself', 'ourselves',  'was', 'through', 'out', 'below', 'own', 'myself', 'theirs', 
             'me', 'why', 'once',  'him', 'than', 'be', 'most', "you'll", 'same', 'some', 'with', 'few', 'it',
             'at', 'after', 'its', 'which', 'there','our', 'this', 'hers', 'being', 'did', 'of', 'had', 'under',
             'over','again', 'where', 'those', 'then', "you're", 'i', 'because', 'does', 'all']

def create_spark_session():
    """Create and return a Spark session"""
    return (SparkSession.builder
            .appName("Amazon Reviews Sentiment Analysis")
            .config("spark.driver.memory", "2g")
            .config("spark.executor.memory", "2g")
            .config("spark.driver.maxResultSize", "2g")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
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

def text_cleaning_udf():
    """Create a UDF for text cleaning using the same method from the notebook"""
    def review_cleaning(text):
        '''Make text lowercase, remove text in square brackets, remove links, remove punctuation
        and remove words containing numbers.'''
        text = str(text).lower()
        text = re.sub('\[.*?\]', '', text)
        text = re.sub('https?://\S+|www\.\S+', '', text)
        text = re.sub('<.*?>+', '', text)
        text = re.sub('[%s]' % re.escape(string.punctuation), '', text)
        text = re.sub('\n', '', text)
        text = re.sub('\w*\d\w*', '', text)
        return text
    
    return udf(review_cleaning, StringType())

def calculate_polarity_udf():
    """Create a UDF for calculating text polarity using TextBlob"""
    def get_polarity(text):
        try:
            return float(TextBlob(str(text)).sentiment.polarity)
        except:
            return 0.0
    
    return udf(get_polarity, FloatType())

def preprocess_data(df):
    """Preprocess the data for training using approach from the notebook"""
    logger.info("Preprocessing data")
    
    # Fill NA values in reviewText
    df = df.na.fill("Missing", ["reviewText"])
    
    # Combine reviewText and summary to create 'reviews' column
    df = df.withColumn("reviews", concat(col("reviewText"), col("summary")))
    
    # Define sentiment labels based on overall rating (following notebook approach)
    df = df.withColumn("sentiment", 
                      when(col("overall") == 3.0, 1)  # Neutral
                     .when((col("overall") == 1.0) | (col("overall") == 2.0), 0)  # Negative
                     .otherwise(2))  # Positive (4.0-5.0)
    
    # Filter out rows with missing review text
    df = df.filter(col("reviews").isNotNull())
    
    # Clean text using the UDF defined earlier
    clean_text = text_cleaning_udf()
    df = df.withColumn("reviews", clean_text(col("reviews")))
    
    # Calculate polarity
    polarity_udf = calculate_polarity_udf()
    df = df.withColumn("polarity", polarity_udf(col("reviews")))
    
    # Add review length and word count features
    df = df.withColumn("review_len", length(col("reviews")))
    
    # Create word count column - need to split text and count
    count_tokens = udf(lambda x: len(str(x).split()), IntegerType())
    df = df.withColumn("word_count", count_tokens(col("reviews")))
    
    # Display class distribution
    class_counts = df.groupBy("sentiment").count().orderBy("sentiment")
    logger.info("Sentiment class distribution:")
    class_counts.show()
    
    # Select relevant columns for modeling
    df = df.select(
        col("reviews"),
        col("polarity"),
        col("review_len"),
        col("word_count"),
        col("sentiment")
    )
    
    return df

def create_pipeline():
    """Create ML pipeline for text processing and model training - adapted from notebook approach"""
    logger.info("Creating ML pipeline")
    
    # Use native Spark tokenization
    tokenizer = Tokenizer(inputCol="reviews", outputCol="words")
    
    # Remove custom stop words
    remover = StopWordsRemover(inputCol="words", outputCol="filtered_words", stopWords=STOP_WORDS)
    
    # Use TF-IDF for feature extraction (similar to notebook)
    vectorizer = CountVectorizer(inputCol="filtered_words", outputCol="raw_features", 
                                maxDF=0.8, minDF=5)
    
    # Apply IDF like the notebook
    idf = IDF(inputCol="raw_features", outputCol="features", minDocFreq=5)
    
    # Logistic Regression with hyperparameters similar to notebook's best model
    lr = LogisticRegression(
        maxIter=100, 
        regParam=0.01,  # Similar to C=10000 in notebook
        elasticNetParam=0.0,  # No L1 regularization like notebook
        labelCol="sentiment", 
        featuresCol="features",
        family="multinomial"
    )
    
    # Random Forest classifier
    rf = RandomForestClassifier(
        numTrees=100, 
        maxDepth=10, 
        seed=42, 
        labelCol="sentiment", 
        featuresCol="features", 
        featureSubsetStrategy="sqrt"
    )
    
    # Naive Bayes classifier
    nb = NaiveBayes(
        smoothing=1.0, 
        labelCol="sentiment", 
        featuresCol="features",
        modelType="multinomial"
    )
    
    # Create pipelines for each model
    lr_pipeline = Pipeline(stages=[tokenizer, remover, vectorizer, idf, lr])
    rf_pipeline = Pipeline(stages=[tokenizer, remover, vectorizer, idf, rf])
    nb_pipeline = Pipeline(stages=[tokenizer, remover, vectorizer, idf, nb])
    
    return {
        "LogisticRegression": lr_pipeline,
        "RandomForest": rf_pipeline,
        "NaiveBayes": nb_pipeline
    }

def plot_confusion_matrix(predictions_df, labels_count):
    """Create and save confusion matrix plot - similar to notebook's approach"""
    logger.info("Creating confusion matrix visualization")
    
    try:
        # Get predictions and actual labels
        y_pred = np.array([float(pred.prediction) for pred in predictions_df.select("prediction").collect()])
        y_true = np.array([float(true.sentiment) for true in predictions_df.select("sentiment").collect()])
        
        # Calculate confusion matrix using sklearn for better visualization
        cm = confusion_matrix(y_true, y_pred)
        cm_normalized = confusion_matrix(y_true, y_pred, normalize='true')
        
        # Set matplotlib backend to avoid display issues
        plt.switch_backend('agg')
        
        # Create figure and subplots
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 8))
        
        # Plot absolute confusion matrix
        sns.heatmap(cm, annot=True, fmt='d', cmap='Blues',
                    xticklabels=['Negative', 'Neutral', 'Positive'],
                    yticklabels=['Negative', 'Neutral', 'Positive'],
                    ax=ax1)
        ax1.set_xlabel('Predicted labels')
        ax1.set_ylabel('True labels')
        ax1.set_title('Confusion Matrix (Counts)')
        
        # Plot normalized confusion matrix
        sns.heatmap(cm_normalized, annot=True, fmt='.2f', cmap='Blues',
                    xticklabels=['Negative', 'Neutral', 'Positive'],
                    yticklabels=['Negative', 'Neutral', 'Positive'],
                    ax=ax2)
        ax2.set_xlabel('Predicted labels')
        ax2.set_ylabel('True labels')
        ax2.set_title('Confusion Matrix (Normalized)')
        
        plt.tight_layout()
        
        # Save the plot
        logger.info(f"Saving confusion matrix to {CONFUSION_MATRIX_PATH}")
        plt.savefig(CONFUSION_MATRIX_PATH, dpi=300)
        plt.close()
        
        # Generate detailed confusion metrics
        report = classification_report(
            y_true, 
            y_pred, 
            target_names=['Negative', 'Neutral', 'Positive'],
            output_dict=True
        )
        
        # Save detailed confusion report as text
        with open(CONFUSION_REPORT_PATH, 'w') as f:
            f.write("# Detailed Classification Metrics\n\n")
            
            # Write the classification report as a formatted table
            f.write("## Classification Report\n")
            report_df = pd.DataFrame(report).transpose()
            f.write(str(report_df))
            
            # Write confusion matrices
            f.write("\n\n## Raw Confusion Matrix\n")
            cm_df = pd.DataFrame(cm, 
                        index=['True Negative', 'True Neutral', 'True Positive'],
                        columns=['Pred Negative', 'Pred Neutral', 'Pred Positive'])
            f.write(str(cm_df))
            
            f.write("\n\n## Normalized Confusion Matrix (by row)\n")
            cm_norm_df = pd.DataFrame(cm_normalized, 
                        index=['True Negative', 'True Neutral', 'True Positive'],
                        columns=['Pred Negative', 'Pred Neutral', 'Pred Positive'])
            f.write(str(cm_norm_df))
            
    except Exception as e:
        logger.error(f"Error creating confusion matrix: {str(e)}")
        # Return a simple confusion matrix if visualization fails
        cm = np.zeros((labels_count, labels_count))
    
    return cm

def calculate_specificity(confusion_matrix, class_label, num_classes):
    """
    Calculate specificity (true negative rate) for a class in a multiclass setting
    Specificity = TN / (TN + FP)
    In multiclass: TN = all samples not in this class and not predicted as this class
    FP = all samples not in this class but predicted as this class
    """
    tn = 0
    fp = 0
    
    for i in range(num_classes):
        for j in range(num_classes):
            if i != class_label and j != class_label:
                # True negatives: not in this class, not predicted as this class
                tn += confusion_matrix[i][j]
            elif i != class_label and j == class_label:
                # False positives: not in this class but predicted as this class
                fp += confusion_matrix[i][j]
    
    return tn / (tn + fp) if (tn + fp) > 0 else 0.0

def save_model_statistics(metrics, confusion_matrix, model_name, num_classes=3):
    """Calculate and save model statistics to a file"""
    logger.info(f"Calculating and saving model statistics to {MODEL_STATS_PATH}")
    
    # Get the overall metrics
    overall_accuracy = metrics.accuracy
    overall_precision = metrics.weightedPrecision
    overall_recall = metrics.weightedRecall
    overall_f1 = metrics.weightedFMeasure()
    
    # Calculate per-class metrics including specificity
    class_metrics = []
    class_names = ['Negative', 'Neutral', 'Positive']
    
    for i in range(num_classes):
        class_precision = metrics.precision(float(i))
        class_recall = metrics.recall(float(i))
        class_f1 = metrics.fMeasure(float(i))
        class_specificity = calculate_specificity(confusion_matrix, i, num_classes)
        
        class_metrics.append({
            'class': class_names[i],
            'precision': class_precision,
            'recall': class_recall,
            'f1_score': class_f1,
            'specificity': class_specificity
        })
    
    # Create the statistics file
    try:
        with open(MODEL_STATS_PATH, 'w') as f:
            f.write(f"# Amazon Review Sentiment Analysis - Model Statistics\n")
            f.write(f"Model: {model_name}\n\n")
            
            f.write(f"## Overall Metrics\n")
            f.write(f"Accuracy: {overall_accuracy:.4f}\n")
            f.write(f"Weighted Precision: {overall_precision:.4f}\n")
            f.write(f"Weighted Recall: {overall_recall:.4f}\n")
            f.write(f"Weighted F1-Score: {overall_f1:.4f}\n\n")
            
            f.write(f"## Per-Class Metrics\n")
            for cls in class_metrics:
                f.write(f"\n### {cls['class']} Class\n")
                f.write(f"Precision: {cls['precision']:.4f}\n")
                f.write(f"Recall: {cls['recall']:.4f}\n")
                f.write(f"F1-Score: {cls['f1_score']:.4f}\n")
                f.write(f"Specificity: {cls['specificity']:.4f}\n")
            
            # Include confusion matrix data
            f.write("\n## Confusion Matrix\n")
            f.write(f"Format: [True Class][Predicted Class]\n")
            for i in range(num_classes):
                for j in range(num_classes):
                    f.write(f"[{class_names[i]}][{class_names[j]}]: {confusion_matrix[i][j]}\n")
    except Exception as e:
        logger.error(f"Error saving model statistics: {str(e)}")

def train_and_evaluate_models(df):
    """Train and evaluate multiple models to find the best one - similar to notebook approach"""
    logger.info("Training and evaluating models")
    
    # Handle class imbalance by resampling 
    # NOTE: This is different from the notebook that used SMOTE
    # We use stratified sampling in Spark as a close alternative
    class_counts = df.groupBy("sentiment").count().collect()
    class_dict = {row['sentiment']: row['count'] for row in class_counts}
    max_count = max(class_dict.values())
    
    # Create a balanced dataset using oversampling of minority classes
    balanced_df = None
    for sentiment_value, count in class_dict.items():
        sentiment_df = df.filter(col("sentiment") == sentiment_value)
        
        # If this class has fewer samples, oversample it
        if count < max_count:
            fraction = max_count / count
            # If we need more than 2x samples, use multiple rounds of sampling with replacement
            if fraction > 2.0:
                sentiment_df = sentiment_df.sample(withReplacement=True, fraction=fraction, seed=42)
            else:
                sentiment_df = sentiment_df.sample(withReplacement=True, fraction=fraction, seed=42)
        
        # Union with previously processed classes
        if balanced_df is None:
            balanced_df = sentiment_df
        else:
            balanced_df = balanced_df.union(sentiment_df)
    
    # Check balanced class distribution
    logger.info("Balanced class distribution:")
    balanced_df.groupBy("sentiment").count().orderBy("sentiment").show()
    
    # Split data into train (80%), validation (10%), and test (10%)
    train_data, temp_data = balanced_df.randomSplit([0.8, 0.2], seed=42)
    val_data, test_data = temp_data.randomSplit([0.5, 0.5], seed=42)
    
    logger.info(f"Train size: {train_data.count()}, Validation size: {val_data.count()}, Test size: {test_data.count()}")
    
    # Create pipelines
    pipelines = create_pipeline()
    
    # Train and evaluate each model
    results = {}
    best_model = None
    best_score = 0.0
    best_model_name = None
    
    # Train each model
    for name, pipeline in pipelines.items():
        logger.info(f"Training {name} model")
        try:
            # For LogisticRegression, use cross-validation to find best hyperparameters
            if name == "LogisticRegression":
                # Define parameter grid (similar to notebook's grid search)
                paramGrid = ParamGridBuilder() \
                    .addGrid(pipeline.getStages()[-1].regParam, [0.01, 0.1, 1.0, 10.0]) \
                    .addGrid(pipeline.getStages()[-1].elasticNetParam, [0.0, 0.5, 1.0]) \
                    .build()
                
                # Create cross validator
                crossval = CrossValidator(estimator=pipeline,
                                         estimatorParamMaps=paramGrid,
                                         evaluator=MulticlassClassificationEvaluator(labelCol="sentiment", 
                                                                                    predictionCol="prediction", 
                                                                                    metricName="accuracy"),
                                         numFolds=5)
                
                # Run cross validation and get best model
                logger.info(f"Running cross-validation for {name}")
                cv_model = crossval.fit(train_data)
                model = cv_model.bestModel
                
            else:
                # For other models, just fit the pipeline
                model = pipeline.fit(train_data)
            
            # Evaluate on validation set
            predictions = model.transform(val_data)
            evaluator = MulticlassClassificationEvaluator(labelCol="sentiment", predictionCol="prediction", metricName="accuracy")
            accuracy = evaluator.evaluate(predictions)
            
            # Also check F1 score
            f1_evaluator = MulticlassClassificationEvaluator(labelCol="sentiment", predictionCol="prediction", metricName="f1")
            f1_score = f1_evaluator.evaluate(predictions)
            
            # Check per-class predictions to ensure model isn't just predicting the majority class
            class_preds = predictions.groupBy("prediction").count().orderBy("prediction")
            logger.info(f"Class predictions distribution for {name}:")
            class_preds.show()
            
            logger.info(f"{name} - Validation Accuracy: {accuracy:.4f}, F1: {f1_score:.4f}")
            results[name] = (model, accuracy, f1_score)
            
            # Keep track of best model
            if accuracy > best_score:
                best_score = accuracy
                best_model = model
                best_model_name = name
                
        except Exception as e:
            logger.error(f"Error training {name} model: {str(e)}")
    
    if best_model_name is None:
        logger.error("All models failed to train. Exiting.")
        sys.exit(1)
    
    # Final evaluation on test set
    logger.info(f"Best model: {best_model_name} with validation accuracy: {best_score:.4f}")
    
    test_predictions = best_model.transform(test_data)
    test_accuracy = evaluator.evaluate(test_predictions)
    f1 = f1_evaluator.evaluate(test_predictions)
    logger.info(f"Final test accuracy: {test_accuracy:.4f}, F1: {f1:.4f}")
    
    # Generate and save confusion matrix
    confusion_matrix = plot_confusion_matrix(test_predictions, 3)  # 3 classes: negative, neutral, positive
    
    # Calculate and log precision, recall for each class
    predictions_and_labels = test_predictions.select("prediction", "sentiment").rdd.map(lambda row: (float(row.prediction), float(row.sentiment)))
    metrics = MulticlassMetrics(predictions_and_labels)
    
    # Save detailed model statistics to a file
    save_model_statistics(metrics, confusion_matrix, best_model_name, num_classes=3)
    
    # Print metrics
    labels = [0.0, 1.0, 2.0]  # Negative, Neutral, Positive
    for label in sorted(labels):
        logger.info(f"Class {label} precision = {metrics.precision(label):.4f}")
        logger.info(f"Class {label} recall = {metrics.recall(label):.4f}")
        logger.info(f"Class {label} F1 score = {metrics.fMeasure(label):.4f}")
    
    return best_model

def save_model(model):
    """Save the trained model"""
    logger.info(f"Saving model to {FINAL_MODEL_PATH}")
    
    # Create directory if it doesn't exist
    os.makedirs(MODEL_DIR, exist_ok=True)
    
    # Save the model
    try:
        model.write().overwrite().save(FINAL_MODEL_PATH)
        logger.info("Model saved successfully")
    except Exception as e:
        logger.error(f"Error saving model: {str(e)}")
        sys.exit(1)

def main():
    """Main function to train the sentiment analysis model"""
    try:
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
    except Exception as e:
        logger.error(f"Error in main process: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()