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
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from bs4 import BeautifulSoup

# List of common user agents
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15'
]

# Create logs directory if it doesn't exist
log_dir = Path("/data/logs")
try:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "collector.log"
except PermissionError:
    # Fallback to current directory if we can't write to /data/logs
    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "collector.log"

# Create screenshots directory if it doesn't exist
screenshot_dir = Path("/data/screenshots")
try:
    screenshot_dir.mkdir(parents=True, exist_ok=True)
except PermissionError:
    # Fallback to current directory if we can't write to /data/screenshots
    screenshot_dir = Path("screenshots")
    screenshot_dir.mkdir(parents=True, exist_ok=True)

# Configure logging
try:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()  # This will continue logging to console
        ]
    )
except PermissionError:
    # Fallback to console-only logging if we can't write to the log file
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler()  # Only log to console
        ]
    )
    logger = logging.getLogger(__name__)
    logger.warning(f"Could not write to log file {log_file}, logging to console only")

logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka1:9092,kafka2:9093')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'amazon-reviews-raw')

def setup_selenium():
    """Set up Selenium WebDriver with Chrome (Chromium)"""
    try:
        logger.info("Configuring Chrome options...")
        chrome_options = Options()
        
        # Set Chrome binary location
        chrome_bin = os.getenv("CHROME_BIN", "/usr/bin/chromium")
        if not os.path.exists(chrome_bin):
            chrome_bin = "/usr/bin/google-chrome"  # Fallback to google-chrome
        chrome_options.binary_location = chrome_bin
        
        # Essential options for running in container
        chrome_options.add_argument('--headless=new')  # Use new headless mode
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--disable-extensions')
        chrome_options.add_argument('--disable-infobars')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--start-maximized')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        
        # Random user agent
        user_agent = random.choice(USER_AGENTS)
        chrome_options.add_argument(f'--user-agent={user_agent}')
        
        # Add additional options to avoid detection
        chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        # Create Service object
        service = Service()
        
        logger.info("Initializing Chrome WebDriver...")
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.set_page_load_timeout(30)
        
        # Execute CDP commands to prevent detection
        driver.execute_cdp_cmd('Network.setUserAgentOverride', {
            "userAgent": user_agent
        })
        
        # Additional CDP commands to make browser appear more human-like
        driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': '''
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5]
                });
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['en-US', 'en']
                });
            '''
        })
        
        logger.info("Chrome WebDriver initialized successfully")
        return driver
    except Exception as e:
        logger.error(f"Failed to setup Selenium: {str(e)}")
        logger.error(f"Error type: {type(e).__name__}")
        if hasattr(e, '__traceback__'):
            import traceback
            logger.error(f"Traceback: {''.join(traceback.format_tb(e.__traceback__))}")
        raise

def setup_kafka_producer():
    """Create and return a Kafka producer instance"""
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(','),
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None
    )
    return producer

def extract_products(driver, max_pages=2):
    """Extract product URLs from Amazon bestsellers page"""
    base_url = f"https://www.amazon.fr/gp/bestsellers/electronics?ie=UTF8&pg="
    product_urls = []
    
    for page_num in range(1, max_pages + 1):
        url = f"{base_url}{page_num}"
        logger.info(f"Scraping products from {url}")
        try:
            driver.get(url)
            
            # Add random delay with human-like behavior
            time.sleep(random.uniform(3, 7))
            
            # Simulate human-like scrolling
            total_height = driver.execute_script("return document.body.scrollHeight")
            for i in range(0, total_height, random.randint(100, 300)):
                driver.execute_script(f"window.scrollTo(0, {i});")
                time.sleep(random.uniform(0.1, 0.3))
            
            # Wait for products to load
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "p13n-sc-uncoverable-faceout"))
            )
            
            # Parse the page with BeautifulSoup
            soup = BeautifulSoup(driver.page_source, 'lxml')
            products = soup.select('div[class="p13n-sc-uncoverable-faceout"]')
            
            for product in products:
                try:
                    # Find the product link
                    product_link = product.select_one('a[href*="/dp/"]')
                    if product_link and 'href' in product_link.attrs:
                        product_url = product_link['href']
                        # Convert relative URL to absolute URL if needed
                        if not product_url.startswith('http'):
                            product_url = f"https://www.amazon.fr{product_url}"
                        # Extract product ID from URL
                        product_id = product_url.split('/dp/')[1].split('/')[0]
                        product_urls.append({
                            'url': product_url,
                            'id': product_id
                        })
                except Exception as e:
                    logger.error(f"Error parsing product: {str(e)}")
                    continue
                    
            # Add random delay between pages
            time.sleep(random.uniform(5, 10))
            
        except TimeoutException:
            logger.warning(f"Timeout waiting for page {page_num} to load")
            continue
        except Exception as e:
            logger.error(f"Error scraping page {page_num}: {str(e)}")
            continue
    
    logger.info(f"Extracted {len(product_urls)} product URLs")
    return product_urls

def extract_reviews(driver, product_info, max_pages=5):
    """Extract reviews from Amazon product page"""
    reviews = []
    product_id = product_info['id']
    product_url = product_info['url']
    
    try:
        logger.info(f"Visiting product page: {product_url}")
        driver.get(product_url)
        
        # Add random delay with human-like behavior
        time.sleep(random.uniform(3, 7))
        
        # Simulate human-like scrolling
        total_height = driver.execute_script("return document.body.scrollHeight")
        for i in range(0, total_height, random.randint(100, 300)):
            driver.execute_script(f"window.scrollTo(0, {i});")
            time.sleep(random.uniform(0.1, 0.3))
        
        # Wait for reviews to load with multiple possible selectors
        review_selectors = [
            (By.ID, "cm_cr-review_list"),
            (By.CLASS_NAME, "review-views"),
            (By.CLASS_NAME, "review"),
            (By.CSS_SELECTOR, "[data-hook='review']"),
            (By.CSS_SELECTOR, "div[class*='review']"),
            (By.CSS_SELECTOR, "div[class*='Review']")
        ]
        
        review_list_found = False
        for selector in review_selectors:
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located(selector)
                )
                review_list_found = True
                break
            except TimeoutException:
                continue
        
        if not review_list_found:
            logger.warning("No review list found on product page")
            return reviews
        
        # Parse the page with BeautifulSoup
        soup = BeautifulSoup(driver.page_source, 'lxml')
        
        # Try different selectors for reviews
        review_elements = []
        review_selectors = [
            'div[data-hook="review"]',
            'div.review',
            'div[class*="review"]',
            'div[class*="Review"]',
            'div[class*="customer-review"]',
            'div[class*="CustomerReview"]'
        ]
        
        for selector in review_selectors:
            review_elements = soup.select(selector)
            if review_elements:
                break
        
        if not review_elements:
            logger.warning("No reviews found on product page")
            return reviews
        
        for review in review_elements:
            try:
                # Try different selectors for each review element
                title = ''
                title_selectors = [
                    'a[data-hook="review-title"]',
                    'span[data-hook="review-title"]',
                    'div[class*="review-title"]',
                    'span[class*="review-title"]'
                ]
                for selector in title_selectors:
                    title_elem = review.select_one(selector)
                    if title_elem:
                        title = title_elem.get_text().strip()
                        break
                
                rating = 0
                rating_selectors = [
                    'i[data-hook="review-star-rating"]',
                    'i[data-hook="cmps-review-star-rating"]',
                    'span[class*="star-rating"]',
                    'i[class*="star-rating"]'
                ]
                for selector in rating_selectors:
                    rating_elem = review.select_one(selector)
                    if rating_elem:
                        try:
                            rating_text = rating_elem.get_text()
                            # Handle both French and English formats
                            if 'sur' in rating_text:
                                rating = float(rating_text.split('sur')[0].strip().replace(',', '.'))
                            elif 'out of' in rating_text:
                                rating = float(rating_text.split('out of')[0].strip())
                            break
                        except (ValueError, IndexError):
                            continue
                
                text = ''
                text_selectors = [
                    'span[data-hook="review-body"]',
                    'div[data-hook="review-body"]',
                    'div[class*="review-text"]',
                    'span[class*="review-text"]'
                ]
                for selector in text_selectors:
                    text_elem = review.select_one(selector)
                    if text_elem:
                        text = text_elem.get_text().strip()
                        break
                
                date = ''
                date_selectors = [
                    'span[data-hook="review-date"]',
                    'span[class*="review-date"]',
                    'div[class*="review-date"]'
                ]
                for selector in date_selectors:
                    date_elem = review.select_one(selector)
                    if date_elem:
                        date = date_elem.get_text().strip()
                        break
                
                # Extract review data
                review_data = {
                    'product_id': product_id,
                    'product_url': product_url,
                    'review_id': review.get('id', ''),
                    'title': title,
                    'rating': rating,
                    'text': text,
                    'date': date,
                    'verified': bool(review.select_one('span[data-hook="avp-badge"]')),
                    'scrape_time': datetime.now().isoformat()
                }
                reviews.append(review_data)
            except Exception as e:
                logger.error(f"Error parsing review: {str(e)}")
                continue
                
    except Exception as e:
        logger.error(f"Error processing product {product_id}: {str(e)}")
    
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
        logger.info("Setting up Selenium WebDriver...")
        driver = setup_selenium()
        logger.info("Setting up Kafka producer...")
        producer = setup_kafka_producer()
        
        logger.info("Extracting product URLs...")
        try:
            product_urls = extract_products(driver)
            logger.info(f"Found {len(product_urls)} products to scrape")
        except Exception as e:
            logger.error(f"Failed to extract products: {str(e)}")
            raise
        
        for product_info in product_urls:
            try:
                logger.info(f"Processing product {product_info['id']}")
                reviews = extract_reviews(driver, product_info)
                if reviews:
                    send_reviews_to_kafka(producer, reviews)
                else:
                    logger.warning(f"No reviews found for product {product_info['id']}")
            except Exception as e:
                logger.error(f"Error processing product {product_info['id']}: {str(e)}")
                continue
            
            # Add random delay between products
            time.sleep(random.uniform(5, 15))
            
    except Exception as e:
        logger.error(f"Error in scrape_reviews: {str(e)}")
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Error details: {str(e)}")
        if hasattr(e, '__traceback__'):
            import traceback
            logger.error(f"Traceback: {''.join(traceback.format_tb(e.__traceback__))}")
    finally:
        if driver:
            try:
                driver.quit()
                logger.info("WebDriver closed successfully")
            except Exception as e:
                logger.error(f"Error closing WebDriver: {str(e)}")
        if producer:
            try:
                producer.close()
                logger.info("Kafka producer closed successfully")
            except Exception as e:
                logger.error(f"Error closing Kafka producer: {str(e)}")
            
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