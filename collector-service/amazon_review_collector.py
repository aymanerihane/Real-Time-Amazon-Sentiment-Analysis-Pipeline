#!/usr/bin/env python3
# Amazon Review Collector Service
import os
import json
import time
import logging
import random
import schedule
from datetime import datetime
from pathlib import Path
from kafka import KafkaProducer
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup
from datetime import datetime
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError


# Basic user agents
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
]

# Setup logging
log_dir = Path("/data/logs")
log_dir.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / "collector.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka1:9092,kafka2:9093')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'amazon-reviews-raw')

# MongoDB configuration
MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://root:example@mongodb:27017/amazon_reviews?authSource=admin')
MONGODB_DATABASE = os.getenv('MONGODB_DATABASE', 'amazon_reviews')
MONGODB_COLLECTION = os.getenv('MONGODB_COLLECTION', 'reviews')


def setup_mongodb():
    """Create and return a MongoDB client instance"""
    try:
        client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
        # Test the connection
        client.admin.command('ping')
        logger.info("Successfully connected to MongoDB")
        return client
    except (ConnectionFailure, ServerSelectionTimeoutError) as e:
        logger.error(f"MongoDB connection error: {str(e)}")
        return None


def setup_selenium():
    """Set up Selenium WebDriver with Chrome"""
    chrome_options = Options()
    chrome_options.add_argument('--headless=new')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument(f'--user-agent={random.choice(USER_AGENTS)}')
    
    service = Service()
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.set_page_load_timeout(30)
    return driver

def setup_kafka_producer():
    """Create and return a Kafka producer instance"""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(','),
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None
    )

def extract_products(driver, max_pages=2,max_products=10):
    """Extract product URLs from Amazon bestsellers page"""
    base_url = "https://www.amazon.com/gp/new-releases/kitchen?ie=UTF8&pg="
    product_urls = []
    
    for page_num in range(1, max_pages + 1):
        try:
            url = f"{base_url}{page_num}"
            logger.info(f"Scraping products from {url}")
            driver.get(url)
            time.sleep(random.uniform(2, 4))
            
            # Wait for products to load
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "p13n-sc-uncoverable-faceout"))
            )
            
            soup = BeautifulSoup(driver.page_source, 'lxml')
            products = soup.select('div[class="p13n-sc-uncoverable-faceout"]')
            
            for product in products:
                product_link = product.select_one('a[href*="/dp/"]')
                if product_link and 'href' in product_link.attrs:
                    product_url = product_link['href']
                    if not product_url.startswith('http'):
                        product_url = f"https://www.amazon.com{product_url}"
                    asin = product_url.split('/dp/')[1].split('/')[0]
                    product_urls.append({
                        'url': product_url,
                        'id': asin
                    })
            
            time.sleep(random.uniform(2, 4))
            
        except Exception as e:
            logger.error(f"Error scraping page {page_num}: {str(e)}")
            continue
    
    logger.info(f"Extracted {len(product_urls)} product URLs")
    return product_urls

def extract_reviews(driver, product_info):
    """Extract reviews from Amazon product page"""
    reviews = []
    review_elements = []
    asin = product_info['id']
    product_url = product_info['url']
    # Try different selectors for reviews
    
    try:
        
        logger.info(f"Visiting product page: {product_url}")
        driver.get(product_url)
        time.sleep(random.uniform(2, 4))
        
        # Wait for reviews to load
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "[data-hook='review']"))
        )

        
        soup = BeautifulSoup(driver.page_source, 'lxml')
            
            # Try multiple selectors to handle different Amazon page layouts
        review_elements = soup.select('div[data-hook="review"]')
            
            # If the above selector doesn't find reviews, try alternative selectors
        if not review_elements:
            review_elements = soup.select('div[id^="customer_review-"]')
            logger.warning("reviews geted using div[id^=\"customer_review-\"]")
        
        # If still no reviews, try another common pattern
        if not review_elements:
            review_elements = soup.select('span[data-hook="cr-widget-FocalReviews"] div[id^="customer_review-"]')
            logger.warning("reviews geted using span[data-hook=\"cr-widget-FocalReviews\"] div[id^=\"customer_review-\"]")
            
        if not review_elements:
            logger.warning(f"No reviews found for product {asin} using any selector pattern")

        
        
        for review in review_elements:
            try:
                reviewer_name = review.select_one('span[class="a-profile-name"]').get_text().strip()
                helpful_votes = review.select_one('span[data-hook="helpful-vote-statement"]')
                review_id_text = review.get('id', '')
                review_id = review_id_text.split('customer_review-')[-1]
                helpful = [0, 0]  # Default: 0 people found it helpful out of 0 total votes
                if helpful_votes:
                    helpful_text = helpful_votes.get_text().strip()
                    # Parse English helpful vote text
                    if 'One person found this helpful' in helpful_text:
                        helpful = [1, 1]
                    elif 'people found this helpful' in helpful_text:
                        total_votes = int(''.join(filter(str.isdigit, helpful_text)))
                        helpful = [total_votes, total_votes]
                    elif 'of' in helpful_text and 'found this helpful' in helpful_text:
                        # Format: "X of Y people found this helpful"
                        helpful_parts = helpful_text.split('of')
                        helpful_votes = int(''.join(filter(str.isdigit, helpful_parts[0])))
                        total_parts = helpful_parts[1].split('people')
                        total_votes = int(''.join(filter(str.isdigit, total_parts[0])))
                        helpful = [helpful_votes, total_votes]
                
                title = review.select_one('a[data-hook="review-title"]').find_all("span")[-1].get_text().strip()
                rating_text = review.select_one('i[data-hook="review-star-rating"]').get_text()
                rating = float(rating_text.split('out of')[0].strip().replace(',', '.'))
                
                text = review.select_one('div[data-hook="review-collapsed"]').find_all("span")[0].get_text().strip()
                date_element = review.select_one('span[data-hook="review-date"]')
                date_text = date_element.get_text().strip() if date_element else ""
                date_part = date_text.split("on ")[-1]
                parsed_date = datetime.strptime(date_part, "%B %d, %Y")
                date = parsed_date.strftime("%Y-%m-%d")  # Convert to ISO format
                
                review_data = {
                    "reviewer_name" : reviewer_name,
                    "asin": asin,
                    "review_id": review_id,
                    "title": title,
                    "overall": rating,
                    "reviewText": text,
                    "date": date,
                    "helpful_votes": helpful[0],
                    "total_votes": helpful[1],
                    "scrape_time": datetime.now().strftime("%Y-%m-%d %H:%M")
                }
                reviews.append(review_data)
            except Exception as e:
                logger.error(f"Error parsing review: {str(e)}")
                continue
                
    except Exception as e:
        logger.error(f"Error processing product {asin}: {str(e)}")
    
    logger.info(f"Extracted {len(review_elements)} reviews for product {asin}")
    return reviews

def send_reviews_to_kafka(producer, reviews):
    """Send the extracted reviews to Kafka"""
    for review in reviews:
        try:
            key = f"{review['asin']}_{review['review_id']}"
            producer.send(KAFKA_TOPIC, key=key, value=review)
        except Exception as e:
            logger.error(f"Error sending review to Kafka: {str(e)}")
    
    producer.flush()
    logger.info(f"Successfully sent {len(reviews)} reviews to Kafka")

def store_reviews_in_mongodb(mongo_client, reviews):
    """Store the extracted reviews in MongoDB"""
    if not mongo_client:
        logger.error("MongoDB client is not available")
        return

    try:
        db = mongo_client[MONGODB_DATABASE]
        collection = db[MONGODB_COLLECTION]
        
        # Create unique index on asin and review_id if not exists
        collection.create_index([("asin", 1), ("review_id", 1)], unique=True)
        
        # Insert/update reviews with upsert
        for review in reviews:
            filter_query = {
                "asin": review["asin"],
                "review_id": review["review_id"],
                "reviewer_name" : review["reviewer_name"],
                "title": review["title"],
                "overall": review["overall"],
                "reviewText" : review["reviewText"],
                "date": review["date"],
                "helpful_votes": review["helpful_votes"],
                "total_votes": review["total_votes"],
                "scrape_time": review["scrape_time"]
            }
            update_result = collection.update_one(filter_query, {"$set": review}, upsert=True)
            
        logger.info(f"Successfully stored {len(reviews)} reviews in MongoDB")
    except Exception as e:
        logger.error(f"Error storing reviews in MongoDB: {str(e)}")


def scrape_reviews():
    """Main function to scrape reviews and send to Kafka"""
    logger.info("Starting review scraping job")
    
    driver = None
    producer = None
    
    try:
        driver = setup_selenium()
        producer = setup_kafka_producer()
        mongo_client = setup_mongodb()
        
        product_urls = extract_products(driver)
        logger.info(f"Found {len(product_urls)} products to scrape")
        
        for product_info in product_urls:
            reviews = extract_reviews(driver, product_info)
            if reviews:
                # Send to Kafka
                send_reviews_to_kafka(producer, reviews)

            time.sleep(random.uniform(2, 4))
            
    except Exception as e:
        logger.error(f"Error in scrape_reviews: {str(e)}")
    finally:
        if driver:
            driver.quit()
        if producer:
            producer.close()
            
    logger.info("Completed review scraping job")

def main():
    """Main entry point for the collector service"""
    logger.info("Amazon Review Collector Service starting...")
    scrape_reviews()
    schedule.every(6).hours.do(scrape_reviews)
    
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    main()