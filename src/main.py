import os
import logging
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from extractors.data_extractor import BankingDataExtractor
from transformers.data_transformer import BankingDataTransformer
from loaders.data_loader import BankingDataLoader

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_env_variables():
    """Load environment variables from .env file."""
    load_dotenv()
    return {
        'SOURCE_DB_CONNECTION': os.getenv('SOURCE_DB_CONNECTION'),
        'WAREHOUSE_DB_CONNECTION': os.getenv('WAREHOUSE_DB_CONNECTION')
    }

def truncate_warehouse_tables(engine):
    """Truncate warehouse tables before loading new data."""
    try:
        with engine.begin() as conn:
            # Truncate fact table first due to foreign key constraints
            conn.execute(text("TRUNCATE TABLE fact_transaction;"))
            conn.execute(text("TRUNCATE TABLE dim_customer CASCADE;"))
            
        logger.info("Successfully truncated warehouse tables")
        
    except Exception as e:
        logger.error(f"Error truncating warehouse tables: {str(e)}")
        raise

def main():
    """Main ETL pipeline."""
    logger.info("Starting banking data processing")
    
    try:
        # Load environment variables
        env_vars = load_env_variables()
        source_db = env_vars['SOURCE_DB_CONNECTION']
        warehouse_db = env_vars['WAREHOUSE_DB_CONNECTION']
        
        if not source_db or not warehouse_db:
            raise ValueError("Missing database connection strings")
            
        # Create warehouse engine for truncation
        warehouse_engine = create_engine(warehouse_db)
        
        # Initialize ETL components
        extractor = BankingDataExtractor(source_db)  # Pass connection string
        transformer = BankingDataTransformer(warehouse_db)
        loader = BankingDataLoader(warehouse_db)
        
        logger.info("Starting ETL pipeline")
        
        # Truncate warehouse tables
        truncate_warehouse_tables(warehouse_engine)
        
        # Extract data
        logger.info("Extracting data...")
        customer_data = extractor.extract_customer_data()
        transaction_data = extractor.extract_transaction_data()
        
        # Transform data
        logger.info("Transforming data...")
        transformed_customer_data = transformer.transform_customer_data(customer_data)
        transformed_transaction_data = transformer.transform_transaction_data(transaction_data)
        
        # Load data
        logger.info("Loading data...")
        loader.load_customer_data(transformed_customer_data)
        loader.load_transaction_data(transformed_transaction_data)
        loader.create_indexes()
        
        logger.info("Banking data processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error in ETL pipeline: {str(e)}")
        raise

if __name__ == "__main__":
    main()
