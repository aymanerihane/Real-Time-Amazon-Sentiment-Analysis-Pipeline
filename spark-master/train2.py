from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Initialize Spark session
spark = SparkSession.builder \
    .appName("AmazonReviewSentimentAnalysis") \
    .getOrCreate()

# Load dataset
file_path = "/opt/spark-data/Data.json"  # Replace with your file path
reviews_df = spark.read.csv(file_path, header=True, inferSchema=True)

# Data preprocessing
# Create binary sentiment labels (1=positive, 0=negative)
df = reviews_df.select(["reviewText", "overall"]) \
    .na.drop() \
    .withColumn("sentiment", 
                when(col("overall") >= 4, 1)
                .when(col("overall") <= 2, 0)
                .otherwise(None)) \
    .filter(col("sentiment").isNotNull())

# Split data into training and test sets
(train_data, test_data) = df.randomSplit([0.8, 0.2], seed=42)

# Text processing pipeline
tokenizer = Tokenizer(inputCol="reviewText", outputCol="words")
stopwords_remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
hashing_tf = HashingTF(inputCol="filtered_words", outputCol="raw_features", numFeatures=10000)
idf = IDF(inputCol="raw_features", outputCol="features")

# Model training
lr = LogisticRegression(featuresCol="features", labelCol="sentiment")

# Build pipeline
pipeline = Pipeline(stages=[tokenizer, stopwords_remover, hashing_tf, idf, lr])

# Train model
model = pipeline.fit(train_data)

# Make predictions
predictions = model.transform(test_data)

# Evaluate model
evaluator = MulticlassClassificationEvaluator(labelCol="sentiment", predictionCol="prediction")

accuracy = evaluator.evaluate(predictions, {evaluator.metricName: "accuracy"})
f1 = evaluator.evaluate(predictions, {evaluator.metricName: "f1"})

print(f"Accuracy: {accuracy}")
print(f"F1 Score: {f1}")

# Save model (optional)
model.save("/model/sentiment_model2")

# Stop Spark session
spark.stop()