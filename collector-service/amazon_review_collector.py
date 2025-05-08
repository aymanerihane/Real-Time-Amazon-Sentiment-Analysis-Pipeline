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

# Create logs directory if it doesn't exist
log_dir = Path("/data/logs")
log_dir.mkdir(parents=True, exist_ok=True)
log_file = log_dir / "collector.log"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / "collector.log"),
        logging.StreamHandler()  # This will continue logging to console
    ]
)
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka1:9092,kafka2:9093')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'amazon-reviews-raw')

def setup_selenium():
    """Set up Selenium WebDriver with Chrome (Chromium)"""
    try:
        logger.info("Configuring Chrome options...")
        chrome_options = Options()
        chrome_options.binary_location = os.getenv("CHROME_BIN", "/usr/bin/chromium")
        # chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--disable-extensions')
        chrome_options.add_argument('--disable-infobars')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--start-maximized')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        # Add additional options to avoid detection
        chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        logger.info("Initializing Chrome WebDriver...")
        driver = webdriver.Chrome(options=chrome_options)
        driver.set_page_load_timeout(30)
        
        # Execute CDP commands to prevent detection
        driver.execute_cdp_cmd('Network.setUserAgentOverride', {
            "userAgent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
        driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': '''
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                })
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

def extract_products(driver,max_pages=2):
    base_url = f"https://www.amazon.fr/gp/bestsellers/electronics?ie=UTF8&pg="
    product_ids=[]
    
    for page_num in range(1, max_pages + 1):
        url = f"{base_url}{page_num}"
        logger.info(f"Scraping products from {url}")
        try:
            driver.get(url)
            # Add random delay to avoid detection
            time.sleep(random.uniform(3, 7))
            
            # Wait for products to load
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "p13n-sc-uncoverable-faceout"))
            )
            
            # Parse the page with BeautifulSoup
            soup = BeautifulSoup(driver.page_source, 'lxml')
            products = soup.select('div[class="p13n-sc-uncoverable-faceout"]')
            
            for product in products:
                try:
                    product_id = product.get('id', '')
                    if product_id:
                        product_ids.append(product_id)
                except Exception as e:
                    logger.error(f"Error parsing product: {str(e)}")
                    continue
        except TimeoutException:
            logger.warning(f"Timeout waiting for page {page_num} to load")
            continue
        except Exception as e:
            logger.error(f"Error scraping page {page_num}: {str(e)}")
            continue
    
    logger.info(f"Extracted {len(product_ids)} product IDs")
    return product_ids

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

    
def amazon_login(driver, email, password):
    """Login to Amazon with retry mechanism"""
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            logger.info(f"Attempting Amazon login (attempt {retry_count + 1}/{max_retries})")
            
            # Go to Amazon homepage first
            driver.get("https://www.amazon.com")
            time.sleep(random.uniform(2, 4))
            
            # Take screenshot of homepage
            screenshot_path = f"/data/logs/amazon_login_attempt_{retry_count + 1}_homepage.png"
            driver.save_screenshot(screenshot_path)
            logger.info(f"Saved homepage screenshot to {screenshot_path}")
            
            # Click on sign in
            sign_in_link = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.ID, "nav-link-accountList"))
            )
            sign_in_link.click()
            time.sleep(random.uniform(1, 2))
            
            # Take screenshot of sign in page
            screenshot_path = f"/data/logs/amazon_login_attempt_{retry_count + 1}_signin_page.png"
            driver.save_screenshot(screenshot_path)
            logger.info(f"Saved sign in page screenshot to {screenshot_path}")
            
            # Wait for email field and enter email
            email_field = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "ap_email_login"))
            )
            email_field.clear()
            for char in email:
                email_field.send_keys(char)
                time.sleep(random.uniform(0.1, 0.3))
            time.sleep(random.uniform(0.5, 1))
            
            # Click continue
            continue_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.ID, "continue"))
            )
            continue_button.click()
            time.sleep(random.uniform(1, 2))
            
            # Take screenshot after email entry
            screenshot_path = f"/data/logs/amazon_login_attempt_{retry_count + 1}_after_email.png"
            driver.save_screenshot(screenshot_path)
            logger.info(f"Saved after email screenshot to {screenshot_path}")
            
            # Wait for password field and enter password
            password_field = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "ap_password"))
            )
            password_field.clear()
            for char in password:
                password_field.send_keys(char)
                time.sleep(random.uniform(0.1, 0.3))
            time.sleep(random.uniform(0.5, 1))
            
            # Click sign in
            sign_in_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.ID, "signInSubmit"))
            )
            sign_in_button.click()
            
            # Wait for successful login
            time.sleep(5)
            
            # Take screenshot after login attempt
            screenshot_path = f"/data/logs/amazon_login_attempt_{retry_count + 1}_after_login.png"
            driver.save_screenshot(screenshot_path)
            logger.info(f"Saved after login screenshot to {screenshot_path}")
            
            # Verify login success
            if "Hello, Sign in" not in driver.page_source:
                logger.info("Successfully logged in to Amazon")
                return True
            else:
                logger.warning("Login verification failed")
                retry_count += 1
                time.sleep(random.uniform(5, 10))
                
        except Exception as e:
            logger.error(f"Login attempt {retry_count + 1} failed: {str(e)}")
            # Take screenshot on error
            try:
                screenshot_path = f"/data/logs/amazon_login_attempt_{retry_count + 1}_error.png"
                driver.save_screenshot(screenshot_path)
                logger.info(f"Saved error screenshot to {screenshot_path}")
            except Exception as screenshot_error:
                logger.error(f"Failed to save error screenshot: {str(screenshot_error)}")
            
            retry_count += 1
            time.sleep(random.uniform(5, 10))
    
    raise Exception("Failed to login after maximum retries")

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
        
        logger.info("Attempting Amazon login...")
        try:
            amazon_login(driver, os.getenv('AMAZON_EMAIL'), os.getenv('AMAZON_PASSWORD'))
            logger.info("Amazon login successful")
        except Exception as e:
            logger.error(f"Failed to login to Amazon: {str(e)}")
            raise
        
        logger.info("Extracting products...")
        try:
            product_ids = extract_products(driver)
            logger.info(f"Found {len(product_ids)} products to scrape")
        except Exception as e:
            logger.error(f"Failed to extract products: {str(e)}")
            raise
        
        for product_id in product_ids:
            try:
                logger.info(f"Processing product {product_id}")
                reviews = extract_reviews(driver, product_id)
                if reviews:
                    send_reviews_to_kafka(producer, reviews)
                else:
                    logger.warning(f"No reviews found for product {product_id}")
            except Exception as e:
                logger.error(f"Error processing product {product_id}: {str(e)}")
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