# from sqlalchemy.orm import Session
from fastapi import HTTPException
from database import Database
from typing import List, Dict, Any
from datetime import datetime, UTC

class ReviewService:
    @staticmethod
    async def create_review(review_data: Dict[str, Any]) -> Dict[str, Any]:
        collection = await Database.get_collection("reviews")
        review_text = review_data["reviewText"]
        review_text = review_text.replace("\n", " ")
        review_data["overall"] = review_data["overall"]
        review_data["reviewer_name"] = review_data["reviewer_name"]
        review_data["created_at"] = datetime.now(UTC).isoformat()
        result = await collection.insert_one(review_data)
        review_data["_id"] = str(result.inserted_id)
        return review_data

    @staticmethod
    async def get_reviews(skip: int = 0, limit: int = 10) -> List[Dict[str, Any]]:
        collection = await Database.get_collection("reviews")
        cursor = collection.find().skip(skip).limit(limit)
        reviews = await cursor.to_list(length=limit)
        return reviews

    @staticmethod
    async def get_review_by_id(review_id: str) -> Dict[str, Any]:
        collection = await Database.get_collection("reviews")
        review = await collection.find_one({"review_id": review_id})
        if not review:
            raise HTTPException(status_code=404, detail="Review not found")
        return review

    @staticmethod
    async def get_reviews_by_product(asin: str, skip: int = 0, limit: int = 10) -> List[Dict[str, Any]]:
        collection = await Database.get_collection("reviews")
        cursor = collection.find({"asin": asin}).skip(skip).limit(limit)
        reviews = await cursor.to_list(length=limit)
        return reviews

class SentimentService:
    @staticmethod
    async def create_sentiment_analysis(sentiment_data: Dict[str, Any]) -> Dict[str, Any]:
        collection = await Database.get_collection("sentiment_analysis")
        sentiment_data["created_at"] = datetime.now(UTC).isoformat()
        result = await collection.insert_one(sentiment_data)
        sentiment_data["_id"] = str(result.inserted_id)
        return sentiment_data

    @staticmethod
    async def get_sentiment_by_review_id(review_id: str) -> Dict[str, Any]:
        collection = await Database.get_collection("sentiment_analysis")
        sentiment = await collection.find_one({"review_id": review_id})
        if not sentiment:
            raise HTTPException(status_code=404, detail="Sentiment analysis not found")
        return sentiment

    @staticmethod
    async def get_sentiment_stats() -> Dict[str, Any]:
        collection = await Database.get_collection("sentiment_analysis")
        pipeline = [
            {
                "$group": {
                    "_id": "$sentiment_label",
                    "count": {"$sum": 1},
                    "avg_score": {"$avg": "$sentiment_score"}
                }
            }
        ]
        cursor = collection.aggregate(pipeline)
        stats = await cursor.to_list(length=None)
        return {
            "sentiment_distribution": stats,
            "total_analyzed": sum(stat["count"] for stat in stats)
        }
