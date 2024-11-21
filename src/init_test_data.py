import os
import logging
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta

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

def create_source_tables(engine):
    """Create source database tables."""
    try:
        with engine.begin() as conn:
            # Create customers table
            conn.execute(text("""
                DROP TABLE IF EXISTS customers CASCADE;
                CREATE TABLE customers (
                    customer_id SERIAL PRIMARY KEY,
                    first_name VARCHAR(100),
                    last_name VARCHAR(100),
                    email VARCHAR(255) UNIQUE,
                    phone VARCHAR(20),
                    address TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """))
            
            # Create transactions table
            conn.execute(text("""
                DROP TABLE IF EXISTS transactions CASCADE;
                CREATE TABLE transactions (
                    transaction_id SERIAL PRIMARY KEY,
                    customer_id INTEGER REFERENCES customers(customer_id),
                    amount DECIMAL(15,2),
                    transaction_type VARCHAR(50),
                    transaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status VARCHAR(20),
                    description TEXT
                );
            """))
            
            logger.info("Created source database tables")
            
    except Exception as e:
        logger.error(f"Error creating source tables: {str(e)}")
        raise

def insert_test_data(engine):
    """Insert test data into source database."""
    try:
        with engine.begin() as conn:
            # Insert test customers
            conn.execute(text("""
                INSERT INTO customers (first_name, last_name, email, phone, address, created_at)
                VALUES 
                    ('John', 'Doe', 'john.doe@email.com', '1234567890', '123 Main St', CURRENT_TIMESTAMP),
                    ('Jane', 'Smith', 'jane.smith@email.com', '0987654321', '456 Oak Ave', CURRENT_TIMESTAMP);
            """))
            
            # Insert test transactions
            conn.execute(text("""
                INSERT INTO transactions (customer_id, amount, transaction_type, transaction_date, status, description)
                VALUES 
                    (1, 1000.00, 'DEPOSIT', CURRENT_TIMESTAMP, 'COMPLETED', 'Initial deposit'),
                    (1, -50.00, 'WITHDRAWAL', CURRENT_TIMESTAMP, 'COMPLETED', 'ATM withdrawal'),
                    (2, 2000.00, 'DEPOSIT', CURRENT_TIMESTAMP, 'COMPLETED', 'Initial deposit'),
                    (2, -150.00, 'TRANSFER', CURRENT_TIMESTAMP, 'COMPLETED', 'Transfer to savings');
            """))
            
            logger.info("Inserted test data into source database")
            
    except Exception as e:
        logger.error(f"Error inserting test data: {str(e)}")
        raise

def main():
    """Initialize source database with test data."""
    try:
        # Load environment variables
        env_vars = load_env_variables()
        source_db = env_vars['SOURCE_DB_CONNECTION']
        
        if not source_db:
            raise ValueError("Missing source database connection string")
        
        # Create engine
        engine = create_engine(source_db)
        
        # Create tables and insert test data
        create_source_tables(engine)
        insert_test_data(engine)
        
        logger.info("Successfully initialized source database with test data")
        
    except Exception as e:
        logger.error(f"Error in initialization process: {str(e)}")
        raise

if __name__ == "__main__":
    main()
