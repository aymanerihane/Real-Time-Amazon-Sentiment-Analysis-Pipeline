#!/usr/bin/env python3
# Amazon Review Collector Service
import os
import json
import time
import logging
import random
import schedule
from datetime import datetime
from kafka import KafkaProducer
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka1:9092,kafka2:9093')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'amazon-reviews-raw')

# Amazon product IDs to scrape reviews from
PRODUCT_IDS = [
    'B07ZPKN6YR',  # Example product ID - Replace with actual product IDs
    'B07ZPKBL9H',
    'B07ZPK5W64'
]

def setup_selenium():
    """Set up Selenium WebDriver with Chrome (Chromium)"""
    chrome_options = Options()
    chrome_options.binary_location = os.getenv("CHROME_BIN", "/usr/bin/chromium")
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')

    driver = webdriver.Chrome(options=chrome_options)
    return driver

def setup_kafka_producer():
    """Create and return a Kafka producer instance"""
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(','),
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None
    )
    return producer

def extract_reviews(driver, product_id, max_pages=5):
    """Extract reviews from Amazon product page"""
    reviews = []
    base_url = f"https://www.amazon.com/product-reviews/{product_id}/ref=cm_cr_arp_d_viewopt_srt?sortBy=recent&pageNumber="
    
    for page_num in range(1, max_pages + 1):
        url = f"{base_url}{page_num}"
        logger.info(f"Scraping reviews from {url}")
        
        try:
            driver.get(url)
            # Add random delay to avoid detection
            time.sleep(random.uniform(3, 7))
            
            # Wait for reviews to load
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "cm_cr-review_list"))
            )
            
            # Parse the page with BeautifulSoup
            soup = BeautifulSoup(driver.page_source, 'lxml')
            review_elements = soup.select('div[data-hook="review"]')
            
            for review in review_elements:
                try:
                    # Extract review data
                    review_data = {
                        'product_id': product_id,
                        'review_id': review.get('id', ''),
                        'title': review.select_one('a[data-hook="review-title"]').get_text().strip() if review.select_one('a[data-hook="review-title"]') else '',
                        'rating': float(review.select_one('i[data-hook="review-star-rating"]').get_text().split(' out of')[0]) if review.select_one('i[data-hook="review-star-rating"]') else 0,
                        'text': review.select_one('span[data-hook="review-body"]').get_text().strip() if review.select_one('span[data-hook="review-body"]') else '',
                        'date': review.select_one('span[data-hook="review-date"]').get_text().strip() if review.select_one('span[data-hook="review-date"]') else '',
                        'verified': bool(review.select_one('span[data-hook="avp-badge"]')),
                        'scrape_time': datetime.now().isoformat()
                    }
                    reviews.append(review_data)
                except Exception as e:
                    logger.error(f"Error parsing review: {str(e)}")
                    continue
            
        except TimeoutException:
            logger.warning(f"Timeout waiting for page {page_num} to load")
            continue
        except Exception as e:
            logger.error(f"Error scraping page {page_num}: {str(e)}")
            continue
            
    logger.info(f"Extracted {len(reviews)} reviews for product {product_id}")
    return reviews

def send_reviews_to_kafka(producer, reviews):
    """Send the extracted reviews to Kafka"""
    for review in reviews:
        try:
            # Use product_id + review_id as key for proper partitioning
            key = f"{review['product_id']}_{review['review_id']}"
            producer.send(KAFKA_TOPIC, key=key, value=review)
            logger.debug(f"Sent review {key} to Kafka")
        except Exception as e:
            logger.error(f"Error sending review to Kafka: {str(e)}")
    
    # Make sure all messages are sent
    producer.flush()
    logger.info(f"Successfully sent {len(reviews)} reviews to Kafka")

def scrape_reviews():
    """Main function to scrape reviews and send to Kafka"""
    logger.info("Starting review scraping job")
    
    driver = None
    producer = None
    
    try:
        driver = setup_selenium()
        producer = setup_kafka_producer()
        
        for product_id in PRODUCT_IDS:
            reviews = extract_reviews(driver, product_id)
            if reviews:
                send_reviews_to_kafka(producer, reviews)
            
            # Add random delay between products
            time.sleep(random.uniform(5, 15))
            
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
    
    # Run immediately at startup
    scrape_reviews()
    
    # Schedule to run every 6 hours
    schedule.every(6).hours.do(scrape_reviews)
    
    # Keep the script running
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    main()