
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List
import asyncio
import json
from database import Database
from service import ReviewService, SentimentService
from kafka_client import consume_messages, consume_sentiment_results
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = FastAPI(title="Amazon Reviews API")

# CORS middleware configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with specific origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# WebSocket connection manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except WebSocketDisconnect:
                self.disconnect(connection)

manager = ConnectionManager()

# Pydantic models for request/response validation
class ReviewBase(BaseModel):
    reviewerID: str
    asin: str
    reviewer_name: str
    title: str
    overall: float
    reviewText: str
    date: str
    helpful_votes: int
    total_votes: int
    scrape_time: str

class SentimentAnalysis(BaseModel):
    reviewerID: str
    asin: str
    reviewer_name: str
    title: str
    overall: float
    reviewText: str
    date: str
    helpful_votes: int
    total_votes: int
    scrape_time: str
    sentiment: str
    processed_at: str

# # Kafka consumer for raw reviews

# async def start_kafka_consumer():
#     logger.info("Starting Kafka consumer for raw reviews")
#     while True:
#         try:
#             logger.debug("Attempting to consume raw review message...")
#             review_data = await asyncio.to_thread(consume_messages)
#             if review_data:
#                 logger.info(f"Received new review for product: {review_data.get('asin', 'unknown')}")
#                 # Store raw review in MongoDB
#                 await ReviewService.create_review(review_data)
                
#                 # Broadcast raw review to WebSocket clients
#                 await manager.broadcast({
#                     "type": "new_review",
#                     "data": review_data
#                 })
#                 logger.debug("Successfully processed and broadcasted review")
#             else:
#                 logger.debug("No new raw review messages available")
#         except Exception as e:
#             logger.error(f"Error in Kafka consumer: {str(e)}")
#         await asyncio.sleep(1)


# # Kafka consumer for sentiment results
async def start_sentiment_consumer():
    logger.info("Starting Kafka consumer for sentiment results")
    while True:
        try:
            logger.debug("Attempting to consume sentiment result...")

            sentiment_data = await asyncio.to_thread(consume_messages)
            if sentiment_data:
                logger.info(f"Received sentiment analysis for review: {sentiment_data.get('reviewerID', 'unknown')}")
                # Store sentiment analysis in MongoDB
                await SentimentService.create_sentiment_analysis(sentiment_data)
                
                # Broadcast sentiment analysis to WebSocket clients
                await manager.broadcast({
                    "type": "new_sentiment",
                    "data": sentiment_data
                })
                logger.debug("Successfully processed and broadcasted sentiment analysis")
            else:
                logger.debug("No new sentiment results available")
        except Exception as e:
            logger.error(f"Error in sentiment consumer: {str(e)}")
        await asyncio.sleep(1)

# # WebSocket endpoints
@app.websocket("/ws/reviews")
async def websocket_reviews_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            try:
                message = json.loads(data)
                if message.get("type") == "subscribe":
                    await websocket.send_json({
                        "type": "product_reviews",
                        "data": message
                    })
            except json.JSONDecodeError:
                await websocket.send_json({
                    "type": "error",
                    "message": "Invalid JSON format"
                })
    except WebSocketDisconnect:
        manager.disconnect(websocket)

# # REST API Endpoints
@app.on_event("startup")
async def startup_event():
    logger.debug("Starting application...")
    try:
        print("Connecting to the database...")
        await Database.connect_db()
        logger.debug("Database connected successfully")
        
        print("Starting Kafka consumers...")
        asyncio.create_task(start_sentiment_consumer())
        logger.debug("Kafka consumers started")
    except Exception as e:
        logger.error(f"Error during startup: {str(e)}")
        raise

@app.on_event("shutdown")
async def shutdown_event():
    print("Closing database connection...")
    await Database.close_db()

@app.get("/")
async def root():
    logger.debug("Root endpoint called")
    return {"message": "Amazon Reviews API is running"}

@app.get("/health")
async def health_check():
    logger.debug("Health check endpoint called")
    return {"status": "healthy"}




