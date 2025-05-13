#!/usr/bin/env python3
import os
import logging
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report
from imblearn.over_sampling import SMOTE
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
import re
import joblib
from sklearn.ensemble import RandomForestClassifier
from sklearn.svm import SVC
import json

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
MODEL_PATH = '/app/resources/sentiment_model_sklearn.pkl'
DATA_PATH = '/app/data/Data.json'

def preprocess_text(text):
    """Preprocess text data"""
    if not isinstance(text, str):
        return ""
    
    # Convert to lowercase and remove special characters
    text = text.lower()
    text = re.sub(r'[^a-zA-Z\s.,!?]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    
    # Tokenize and lemmatize
    tokens = word_tokenize(text)
    lemmatizer = WordNetLemmatizer()
    stop_words = set(stopwords.words('english'))
    tokens = [lemmatizer.lemmatize(token) for token in tokens if token not in stop_words and len(token) > 1]
    
    return ' '.join(tokens)

def load_data():
    """Load data from JSON file"""
    try:
        # Read the file line by line
        data = []
        with open(DATA_PATH, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:  # Skip empty lines
                    try:
                        data.append(json.loads(line))
                    except json.JSONDecodeError as e:
                        logger.warning(f"Skipping invalid JSON line: {str(e)}")
                        continue
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        logger.info(f"Successfully loaded {len(df)} records from {DATA_PATH}")
        return df
    except Exception as e:
        logger.error(f"Error loading data: {str(e)}")
        raise

def train_model():
    """Train the sentiment analysis model"""
    models = {
        'LogisticRegression': LogisticRegression(max_iter=1000, C=1.0, class_weight='balanced'),
        'RandomForest': RandomForestClassifier(n_estimators=100, max_depth=10, class_weight='balanced'),
        'SVM': SVC(probability=True, kernel='linear', class_weight='balanced')
    }
    
    try:
        logger.info("Starting model training...")
        # Load and preprocess data
        df = load_data()
        df['processed_text'] = df['reviewText'].apply(preprocess_text)
        
        # Convert ratings to sentiment labels
        df['sentiment'] = df['overall'].apply(lambda x: 2 if x >= 4 else (0 if x <= 2 else 1))
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            df['processed_text'], df['sentiment'], test_size=0.2, random_state=42
        )
        
        # Vectorize text
        vectorizer = TfidfVectorizer(max_features=5000)
        X_train_vec = vectorizer.fit_transform(X_train)
        X_test_vec = vectorizer.transform(X_test)
        
        # Handle class imbalance
        smote = SMOTE(random_state=42)
        X_train_balanced, y_train_balanced = smote.fit_resample(X_train_vec, y_train)
        
        # Train and evaluate all models
        best_model = None
        best_score = 0
        best_model_name = None
        
        for model_name, model in models.items():
            logger.info(f"Training {model_name}...")
            
            # Train model
            model.fit(X_train_balanced, y_train_balanced)
            
            # Evaluate model
            y_pred = model.predict(X_test_vec)
            score = model.score(X_test_vec, y_test)
            
            logger.info(f"{model_name} accuracy: {score:.4f}")
            logger.info(f"\nClassification Report for {model_name}:\n{classification_report(y_test, y_pred)}")
            
            # Update best model if current model is better
            if score > best_score:
                best_score = score
                best_model = model
                best_model_name = model_name
        
        logger.info(f"Best model: {best_model_name} with accuracy: {best_score:.4f}")
        
        # Save best model and vectorizer
        os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
        joblib.dump((best_model, vectorizer), MODEL_PATH)
        logger.info(f"Best model ({best_model_name}) saved successfully to {MODEL_PATH}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error in model training: {str(e)}")
        return False

def main():
    """Main function"""
    try:
        # Train model
        if not train_model():
            logger.error("Model training failed")
            return
        
        logger.info("Model training completed successfully")
        
    except Exception as e:
        logger.error("Error in main process: %s", str(e))
        raise

if __name__ == "__main__":
    main() 