from motor.motor_asyncio import AsyncIOMotorClient
import os
import asyncio
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Check for local development environment
IS_LOCAL = os.getenv("IS_LOCAL", "false").lower() == "true"

# Determine MongoDB URI based on environment
if IS_LOCAL:
    # Local development MongoDB URI
    MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017/amazon_reviews")
else:
    # Docker MongoDB URI with authentication
    MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://root:example@mongodb:27017/amazon_reviews?authSource=admin")

MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "amazon_reviews")

# MongoDB connection options for better reliability
MONGODB_OPTIONS = {
    "serverSelectionTimeoutMS": 5000,  # 5 seconds for server selection
    "connectTimeoutMS": 10000,         # 10 seconds connection timeout
    "socketTimeoutMS": 45000,          # 45 seconds socket timeout
    "maxPoolSize": 10,                # Maximum connection pool size
    "minPoolSize": 1,                 # Minimum connection pool size
    "maxIdleTimeMS": 30000,           # Close connections after 30 seconds idle
    "retryWrites": True,              # Retry write operations
}

class Database:
    client: AsyncIOMotorClient = None
    db = None
    connected = False

    @classmethod
    async def connect_db(cls):
        try:
            logger.info(f"Connecting to MongoDB at {MONGODB_URI.split('@')[-1]}")  # Only log the server part, not credentials
            
            # Create client with options
            cls.client = AsyncIOMotorClient(
                MONGODB_URI,
                serverSelectionTimeoutMS=MONGODB_OPTIONS["serverSelectionTimeoutMS"],
                connectTimeoutMS=MONGODB_OPTIONS["connectTimeoutMS"],
                socketTimeoutMS=MONGODB_OPTIONS["socketTimeoutMS"],
                maxPoolSize=MONGODB_OPTIONS["maxPoolSize"],
                minPoolSize=MONGODB_OPTIONS["minPoolSize"],
                maxIdleTimeMS=MONGODB_OPTIONS["maxIdleTimeMS"],
                retryWrites=MONGODB_OPTIONS["retryWrites"]
            )
            
            # Test connection with a small timeout
            await asyncio.wait_for(cls.client.admin.command('ping'), timeout=5.0)
            
            cls.db = cls.client[MONGODB_DATABASE]
            cls.connected = True
            logger.info("Successfully connected to MongoDB")
            
            # Try to list collections to verify database access
            collections = await cls.db.list_collection_names()
            logger.info(f"Available collections: {', '.join(collections) if collections else 'None'}")
            
            # Create required collections if they don't exist
            if "sentiment_analysis" not in collections:
                logger.info("Creating sentiment_analysis collection")
                await cls.db.create_collection("sentiment_analysis")
                
        except asyncio.TimeoutError:
            logger.error("MongoDB connection timed out. Check your MongoDB server is running.")
            # Set up a dummy DB for fallback mode
            cls._setup_fallback_mode()
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {str(e)}")
            # Set up a dummy DB for fallback mode
            cls._setup_fallback_mode()

    @classmethod
    def _setup_fallback_mode(cls):
        """Set up a fallback mode with mock data for when MongoDB is unavailable"""
        logger.warning("Setting up fallback mode with mock data")
        from pymongo.collection import Collection
        from pymongo.results import InsertOneResult
        import datetime
        
        # Create a mock client and db that can return some basic data
        class MockCollection:
            async def find_one(self, *args, **kwargs):
                return {"_id": "123", "asin": "B0DM18PPS4", "reviewText": "This is a sample review", "sentiment": 2}
                
            async def find(self, *args, **kwargs):
                # Return a mock cursor
                return MockCursor()
                
            async def count_documents(self, *args, **kwargs):
                return 5  # Return 5 as mock document count
                
            async def insert_one(self, doc, *args, **kwargs):
                # Create a mock InsertOneResult
                doc["_id"] = "new_id_" + datetime.datetime.now().isoformat()
                return InsertOneResult(doc["_id"], acknowledged=True)
        
        # Mock cursor for find() operation
        class MockCursor:
            async def to_list(self, length=None):
                # Return some mock documents
                return [
                    {"_id": "123", "asin": "B0DM18PPS4", "reviewText": "Sample positive review", "sentiment": 2, "sentiment_label": "Positive"},
                    {"_id": "456", "asin": "B0DM18PPS4", "reviewText": "Sample neutral review", "sentiment": 1, "sentiment_label": "Neutral"},
                    {"_id": "789", "asin": "B0DM18PPS4", "reviewText": "Sample negative review", "sentiment": 0, "sentiment_label": "Negative"},
                ]
                
            def skip(self, n):
                return self
                
            def limit(self, n):
                return self
        
        # Create a mock DB with a mock collection
        class MockDB:
            def __getitem__(self, name):
                return MockCollection()
                
            async def list_collection_names(self):
                return ["sentiment_analysis"]
                
            async def create_collection(self, name):
                logger.info(f"Mock creating collection: {name}")
                return None
        
        # Set up the mock database
        cls.db = MockDB()
        cls.connected = True
        logger.info("Running in fallback mode with mock data")

    @classmethod
    async def close_db(cls):
        if cls.client:
            cls.client.close()
            cls.connected = False
            logger.info("MongoDB connection closed")

    @classmethod
    async def get_collection(cls, collection_name: str):
        # Ensure we're connected to the database
        if not cls.connected:
            await cls.connect_db()
        return cls.db[collection_name]
