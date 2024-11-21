import sys
import os
import schedule
import time
import logging
from datetime import datetime
from main import run_etl_pipeline

# Add the project root directory to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def job():
    """
    Job function that will be scheduled to run.
    """
    try:
        logger.info(f"Starting scheduled ETL job at {datetime.now()}")
        run_etl_pipeline()
        logger.info(f"Completed scheduled ETL job at {datetime.now()}")
    except Exception as e:
        logger.error(f"Error in scheduled job: {str(e)}")

def run_scheduler():
    """
    Set up and run the scheduler.
    """
    # Schedule the job to run daily at 1 AM
    schedule.every().day.at("01:00").do(job)
    
    # You can add more scheduled runs if needed:
    # schedule.every().hour.do(job)  # Run every hour
    # schedule.every().monday.do(job)  # Run every Monday
    # schedule.every().day.at("13:00").do(job)  # Run every day at 1 PM
    
    logger.info("Scheduler started. Press Ctrl+C to exit.")
    
    while True:
        try:
            # Run pending jobs
            schedule.run_pending()
            # Sleep for 60 seconds before checking again
            time.sleep(60)
        except KeyboardInterrupt:
            logger.info("Scheduler stopped by user")
            break
        except Exception as e:
            logger.error(f"Error in scheduler: {str(e)}")
            # Sleep for 5 minutes before retrying
            time.sleep(300)

if __name__ == "__main__":
    run_scheduler()
