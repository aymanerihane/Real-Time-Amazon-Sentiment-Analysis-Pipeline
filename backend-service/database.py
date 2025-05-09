from motor.motor_asyncio import AsyncIOMotorClient
import os

MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://root:example@mongodb:27017/amazon_reviews?authSource=admin")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "amazon_reviews")

class Database:
    client: AsyncIOMotorClient = None
    db = None

    @classmethod
    async def connect_db(cls):
        cls.client = AsyncIOMotorClient(MONGODB_URI)
        cls.db = cls.client[MONGODB_DATABASE]

    @classmethod
    async def close_db(cls):
        if cls.client:
            cls.client.close()

    @classmethod
    async def get_collection(cls, collection_name: str):
        return cls.db[collection_name]
