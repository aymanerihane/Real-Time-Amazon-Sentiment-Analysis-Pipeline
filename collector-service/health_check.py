#!/usr/bin/env python3
import os
import sys
import psutil
import logging
from pathlib import Path

# Configure logging
log_dir = Path("/data/logs")
log_dir.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / "health_check.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def is_collector_running():
    """Check if the collector process is running."""
    try:
        # Check for python processes running amazon_review_collector.py
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                if proc.info['name'] == 'python' and any('amazon_review_collector.py' in cmd for cmd in proc.info['cmdline'] if cmd):
                    logger.info(f"Collector process found: PID {proc.info['pid']}")
                    return True
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                continue
        logger.warning("No collector process found")
        return False
    except Exception as e:
        logger.error(f"Error checking process: {str(e)}")
        return False

def check_log_activity():
    """Check if the collector.log file has been modified recently."""
    try:
        log_file = log_dir / "collector.log"
        if not log_file.exists():
            logger.warning("Collector log file does not exist")
            return False
            
        # Check if log has been modified in the last 10 minutes
        modified_time = log_file.stat().st_mtime
        import time
        current_time = time.time()
        if (current_time - modified_time) > 600:  # 10 minutes
            logger.warning(f"Collector log hasn't been updated in {(current_time - modified_time) / 60:.1f} minutes")
            return False
            
        logger.info("Collector log file is being updated regularly")
        return True
    except Exception as e:
        logger.error(f"Error checking log activity: {str(e)}")
        return False

if __name__ == "__main__":
    process_running = is_collector_running()
    logs_active = check_log_activity()
    
    if process_running and logs_active:
        logger.info("Health check passed: Collector is running and active")
        sys.exit(0)
    else:
        logger.error("Health check failed: Collector service has issues")
        sys.exit(1)