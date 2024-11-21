import pandas as pd
from sqlalchemy import create_engine, text
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BankingDataLoader:
    def __init__(self, warehouse_connection_string: str):
        """Initialize the data loader with warehouse connection."""
        self.engine = create_engine(warehouse_connection_string)
        
    def load_customer_data(self, df: pd.DataFrame) -> bool:
        """
        Load customer data into the warehouse.
        
        Args:
            df: Transformed customer DataFrame
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Load data into dim_customer table
            df.to_sql(
                'dim_customer',
                self.engine,
                if_exists='append',
                index=False
            )
            
            logger.info(f"Successfully loaded {len(df)} customer records into dim_customer")
            return True
            
        except Exception as e:
            logger.error(f"Error loading customer data: {str(e)}")
            raise
            
    def load_transaction_data(self, df: pd.DataFrame) -> bool:
        """
        Load transaction data into the warehouse.
        
        Args:
            df: Transformed transaction DataFrame
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Load data into fact_transaction table
            df.to_sql(
                'fact_transaction',
                self.engine,
                if_exists='append',
                index=False
            )
            
            logger.info(f"Successfully loaded {len(df)} transaction records into fact_transaction")
            return True
            
        except Exception as e:
            logger.error(f"Error loading transaction data: {str(e)}")
            raise
            
    def create_indexes(self):
        """Create necessary indexes on the warehouse tables."""
        try:
            with self.engine.begin() as conn:
                # Create indexes on dim_customer
                conn.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_customer_email 
                    ON dim_customer(email);
                    
                    CREATE INDEX IF NOT EXISTS idx_customer_full_name 
                    ON dim_customer(full_name);
                """))
                
                # Create indexes on fact_transaction
                conn.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_transaction_date 
                    ON fact_transaction(transaction_date);
                    
                    CREATE INDEX IF NOT EXISTS idx_transaction_type 
                    ON fact_transaction(transaction_type);
                    
                    CREATE INDEX IF NOT EXISTS idx_transaction_category 
                    ON fact_transaction(transaction_category);
                """))
                
            logger.info("Successfully created indexes on warehouse tables")
            return True
            
        except Exception as e:
            logger.error(f"Error creating indexes: {str(e)}")
            raise
