import os
from dotenv import load_dotenv
from quality.quality_pipeline import QualityPipeline

def main():
    # Load environment variables
    load_dotenv()
    
    # Get database connection strings from environment variables
    source_db = os.getenv('SOURCE_DB_CONNECTION')
    warehouse_db = os.getenv('WAREHOUSE_DB_CONNECTION')
    
    if not source_db or not warehouse_db:
        raise ValueError("Database connection strings not found in environment variables")
    
    # Initialize and run the quality pipeline
    pipeline = QualityPipeline(source_db, warehouse_db)
    pipeline.run_pipeline()

if __name__ == "__main__":
    main()
