from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Response, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import asyncio
import json
from database import Database
from service import ReviewService, SentimentService
from kafka_client import consume_messages
import logging
import time

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = FastAPI(title="Amazon Reviews API")

# CORS middleware configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000", "*"],  # Allow all origins for testing
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Request timing middleware
@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    try:
        # Remove the asyncio.wait_for wrapper here, as it causes issues with other middleware
        response = await call_next(request)
        process_time = time.time() - start_time
        response.headers["X-Process-Time"] = str(process_time)
        return response
    except Exception as e:
        # Handle all exceptions at this level
        logger.error(f"Error handling request: {request.url}, Error: {str(e)}")
        error_content = {
            "error": str(e),
            "detail": "Internal server error",
            "path": str(request.url)
        }
        return Response(
            content=json.dumps(error_content),
            status_code=500,
            media_type="application/json"
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


# # Kafka consumer for sentiment results
async def start_sentiment_consumer():
    logger.info("Starting Kafka consumer for sentiment results")
    while True:
        try:
            logger.debug("Attempting to consume sentiment result...")

            sentiment_data = await asyncio.to_thread(consume_messages)
            if sentiment_data:
                logger.info(f"Received sentiment analysis for review: {sentiment_data.get('reviewerID', 'unknown')}")
                #!! Store sentiment analysis in MongoDB 
                # await SentimentService.create_sentiment_analysis(sentiment_data)
                
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


# GET THE LIST OF REVIEWS
@app.get("/api/reviews")
async def get_reviews(timeRange: str = None, limit: Optional[int] = None):
    try:
        logger.debug(f"Fetching reviews with timeRange: {timeRange}, limit: {limit}")
        
        # Get reviews based on timeRange - no default limit
        if timeRange:
            reviews = await ReviewService.get_reviews_by_time_range(timeRange)
        else:
            # Default to all reviews with no limit
            reviews = await ReviewService.get_all_reviews()
        
        # If no reviews, provide empty array rather than failing
        if not reviews:
            logger.debug("No reviews found, returning empty array")
            return {"reviews": []}
        
        return {"reviews": reviews}
    except Exception as e:
        logger.error(f"Error fetching reviews: {str(e)}")
        # Return 500 status code with error message
        return Response(
            content=json.dumps({"error": str(e), "reviews": []}),
            status_code=500,
            media_type="application/json"
        )

# /sentiment-stats?${queryParams}`
@app.get("/api/sentiment-stats")
async def get_sentiment_stats(
    product_id: str = None,
    reviewer_id: str = None,
    time_range: str = None,
    sentiment: str = None,
    format: str = None
):
    try:
        logger.debug("Fetching sentiment stats with filters")
        filters = {
            "product_id": product_id,
            "reviewer_id": reviewer_id,
            "time_range": time_range,
            "sentiment": sentiment,
            "format": format
        }
        # Remove None values from filters
        filters = {k: v for k, v in filters.items() if v is not None}
        logger.debug(f"Filters applied: {filters}")
        
        # Call the sentiment service to get the stats with a timeout
        stats = await asyncio.wait_for(
            SentimentService.get_sentiment_stats(filters),
            timeout=10.0
        )
        
        # Return actual stats from the database
        return stats
    except asyncio.TimeoutError:
        logger.error("Sentiment stats query timed out")
        # Return default values on timeout to prevent UI from hanging
        return {
            "positive": 10,
            "negative": 5,
            "neutral": 3,
            "total": 18,
            "error": "Query timed out"
        }
    except Exception as e:
        logger.error(f"Error fetching sentiment stats: {str(e)}")
        # Return default values with error
        return {
            "positive": 10,
            "negative": 5,
            "neutral": 3,
            "total": 18,
            "error": str(e)
        }

# /reviews/time-range/${timeRange}
@app.get("/api/reviews/time-range/{time_range}")
async def get_reviews_by_time_range(time_range: str):
    try:
        logger.debug(f"Fetching reviews for time range: {time_range}")
        
        # Use the dedicated time range method
        reviews = await ReviewService.get_reviews_by_time_range(time_range)
        
        return {"reviews": reviews or []}
    except Exception as e:
        logger.error(f"Error fetching reviews by time range: {str(e)}")
        return Response(
            content=json.dumps({"error": str(e), "reviews": []}),
            status_code=500,
            media_type="application/json"
        )

# /reviews/export?format=${format}
@app.get("/api/reviews/export")
async def export_reviews(format: str):
    try:
        logger.debug(f"Exporting reviews in format: {format}")
        if format not in ["json", "csv"]:
            return Response(
                content=json.dumps({"error": "Invalid format. Supported formats are json and csv."}),
                status_code=400,
                media_type="application/json"
            )
        
        # Get all reviews for export - no limit
        reviews = await ReviewService.get_all_reviews()
        
        if format == "json":
            return {"reviews": reviews or []}
        elif format == "csv":
            # Convert to CSV format
            csv_data = "reviewerID,asin,reviewer_name,title,overall,reviewText,date,helpful_votes,total_votes,scrape_time\n"
            for review in reviews or []:
                csv_data += ",".join([str(review.get(key, "")) for key in csv_data.strip().split(",")]) + "\n"
            return {"csv_data": csv_data}
    except Exception as e:
        logger.error(f"Error exporting reviews: {str(e)}")
        return Response(
            content=json.dumps({"error": str(e)}),
            status_code=500,
            media_type="application/json"
        )




