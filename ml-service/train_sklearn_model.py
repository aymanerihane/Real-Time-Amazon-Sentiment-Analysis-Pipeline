#!/usr/bin/env python3
import json
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.svm import SVC
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
import pickle
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
import re
import logging
import os
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Define paths
DATA_DIR = '/app/data'
RESOURCES_DIR = '/app/resources'
RESULTS_DIR = '/app/results'
DATA_FILE = os.path.join(DATA_DIR, 'Data.json')
MODEL_FILE = os.path.join(RESOURCES_DIR, 'sentiment_model_sklearn.pkl')

# Create necessary directories
os.makedirs(RESOURCES_DIR, exist_ok=True)
os.makedirs(RESULTS_DIR, exist_ok=True)

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

def load_and_preprocess_data(file_path):
    """Load and preprocess the data"""
    logger.info(f"Loading data from JSON file: {file_path}")
    
    if not os.path.exists(file_path):
        logger.error(f"Data file not found at: {file_path}")
        sys.exit(1)
    
    try:
        # Read JSON file in chunks to handle large file
        chunks = []
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    chunks.append(json.loads(line))
                except json.JSONDecodeError as e:
                    logger.warning(f"Skipping invalid JSON line: {e}")
                    continue
        
        if not chunks:
            logger.error("No valid data found in the JSON file")
            sys.exit(1)
        
        # Convert to DataFrame
        df = pd.DataFrame(chunks)
        
        # Keep only relevant columns
        df = df[['reviewerID', 'asin', 'reviewText', 'overall', 'summary']]
        
        # Combine review text and summary for better context
        df['combined_text'] = df['summary'] + ' ' + df['reviewText']
        
        # Preprocess text
        logger.info("Preprocessing text data...")
        df['processed_text'] = df['combined_text'].apply(preprocess_text)
        
        # Create sentiment labels (rating > 3 is positive)
        df['sentiment'] = df['overall'].apply(lambda x: 'positive' if x > 3 else 'negative' if x < 3 else 'neutral')
        
        # Log data distribution
        logger.info(f"Total reviews: {len(df)}")
        logger.info(f"Positive reviews: {len(df[df['sentiment'] == 'positive'])}")
        logger.info(f"Negative reviews: {len(df[df['sentiment'] == 'negative'])}")
        logger.info(f"Neutral reviews: {len(df[df['sentiment'] == 'neutral'])}")
        
        return df
    
    except Exception as e:
        logger.error(f"Error loading data: {str(e)}")
        sys.exit(1)

def train_and_evaluate_models(X_train, X_test, y_train, y_test):
    """Train and evaluate different models"""
    models = {
        'LogisticRegression': LogisticRegression(max_iter=1000, C=1.0, class_weight='balanced'),
        'RandomForest': RandomForestClassifier(n_estimators=100, max_depth=10, class_weight='balanced'),
        'SVM': SVC(probability=True, kernel='linear', class_weight='balanced')
    }
    
    best_model = None
    best_accuracy = 0
    results = {}
    
    for name, model in models.items():
        logger.info(f"\nTraining {name}...")
        
        # Create pipeline with TF-IDF vectorizer
        pipeline = Pipeline([
            ('tfidf', TfidfVectorizer(
                max_features=5000,
                min_df=5,
                max_df=0.95,
                ngram_range=(1, 2),
                sublinear_tf=True  # Apply sublinear scaling to TF
            )),
            ('classifier', model)
        ])
        
        # Train the model
        pipeline.fit(X_train, y_train)
        
        # Make predictions
        y_pred = pipeline.predict(X_test)
        
        # Calculate accuracy
        accuracy = accuracy_score(y_test, y_pred)
        logger.info(f"{name} Accuracy: {accuracy:.4f}")
        
        # Print classification report
        logger.info(f"\nClassification Report for {name}:")
        logger.info(classification_report(y_test, y_pred))
        
        # Store results
        results[name] = {
            'model': pipeline,
            'accuracy': accuracy,
            'predictions': y_pred,
            'true_values': y_test
        }
        
        # Update best model
        if accuracy > best_accuracy:
            best_accuracy = accuracy
            best_model = pipeline
    
    # Plot confusion matrix for best model
    best_name = max(results, key=lambda x: results[x]['accuracy'])
    
    
    # Save detailed results
    with open(os.path.join(RESULTS_DIR, 'training_results.txt'), 'w') as f:
        f.write(f"Best Model: {best_name}\n")
        f.write(f"Best Accuracy: {best_accuracy:.4f}\n\n")
        for name, result in results.items():
            f.write(f"Model: {name}\n")
            f.write(f"Accuracy: {result['accuracy']:.4f}\n")
            f.write(classification_report(result['true_values'], result['predictions']))
            f.write("\n" + "="*50 + "\n")
    
    return best_model, results

def save_model(model, filepath):
    """Save the trained model"""
    try:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, 'wb') as f:
            pickle.dump(model, f)
        logger.info(f"Model saved to {filepath}")
    except Exception as e:
        logger.error(f"Error saving model: {str(e)}")
        sys.exit(1)

def main():
    try:
        # Load and preprocess data
        df = load_and_preprocess_data(DATA_FILE)
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            df['processed_text'],
            df['sentiment'],
            test_size=0.2,
            random_state=42,
            stratify=df['sentiment']  # Ensure balanced split
        )
        
        # Train and evaluate models
        best_model, results = train_and_evaluate_models(X_train, X_test, y_train, y_test)
        
        # Save the best model
        save_model(best_model, MODEL_FILE)
        
        logger.info("Training completed successfully!")
        logger.info(f"Model saved to {MODEL_FILE}")
        logger.info(f"Results saved to {RESULTS_DIR}/")
        
        # Exit with success
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"Error in main process: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 