import pandas as pd
from sqlalchemy import create_engine, text
from typing import Dict, List
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BankingDataExtractor:
    def __init__(self, db_connection_string: str):
        """Initialize the data extractor with database connection string."""
        self.engine = create_engine(db_connection_string)
        
    def extract_customer_data(self, batch_size: int = 1000) -> pd.DataFrame:
        """
        Extract customer data from the source database.
        
        Args:
            batch_size: Number of records to extract in each batch
            
        Returns:
            DataFrame containing customer data
        """
        try:
            query = text("""
                SELECT 
                    customer_id,
                    first_name,
                    last_name,
                    email,
                    phone,
                    address,
                    created_at
                FROM customers
                WHERE created_at >= CURRENT_DATE - INTERVAL '1 day'
            """)
            
            df = pd.read_sql(query, self.engine)
            logger.info(f"Extracted {len(df)} customer records")
            return df
            
        except Exception as e:
            logger.error(f"Error extracting customer data: {str(e)}")
            raise
            
    def extract_transaction_data(self, start_date: datetime = None) -> pd.DataFrame:
        """
        Extract transaction data from the source database.
        
        Args:
            start_date: Optional start date for filtering transactions
            
        Returns:
            DataFrame containing transaction data
        """
        try:
            if start_date is None:
                start_date = datetime.now() - timedelta(days=1)
                
            query = text("""
                SELECT 
                    t.transaction_id,
                    t.customer_id,
                    t.amount,
                    t.transaction_type,
                    t.transaction_date,
                    t.status,
                    t.description,
                    c.first_name,
                    c.last_name,
                    c.email
                FROM transactions t
                JOIN customers c ON t.customer_id = c.customer_id
                WHERE t.transaction_date >= :start_date
            """)
            
            df = pd.read_sql(query, self.engine, params={"start_date": start_date})
            logger.info(f"Extracted {len(df)} transaction records")
            return df
            
        except Exception as e:
            logger.error(f"Error extracting transaction data: {str(e)}")
            raise
            
    def validate_data(self, df: pd.DataFrame, validation_rules: Dict) -> bool:
        """
        Validate extracted data against defined rules.
        
        Args:
            df: DataFrame to validate
            validation_rules: Dictionary containing validation rules
            
        Returns:
            Boolean indicating if validation passed
        """
        try:
            # Check for required columns
            missing_cols = set(validation_rules['required_columns']) - set(df.columns)
            if missing_cols:
                logger.error(f"Missing required columns: {missing_cols}")
                return False
                
            # Check for non-null values in required columns
            for col in validation_rules.get('non_null_columns', []):
                if df[col].isnull().any():
                    logger.error(f"Found null values in column: {col}")
                    return False
                    
            # Check data types
            for col, dtype in validation_rules.get('column_types', {}).items():
                if df[col].dtype != dtype:
                    logger.error(f"Column {col} has incorrect type: {df[col].dtype}, expected: {dtype}")
                    return False
                    
            return True
            
        except Exception as e:
            logger.error(f"Error validating data: {str(e)}")
            raise
            
    def extract_and_validate(self, data_type: str, validation_rules: Dict) -> pd.DataFrame:
        """
        Extract and validate data in one step.
        
        Args:
            data_type: Type of data to extract ('customer' or 'transaction')
            validation_rules: Dictionary containing validation rules
            
        Returns:
            Validated DataFrame
        """
        if data_type == 'customer':
            df = self.extract_customer_data()
        elif data_type == 'transaction':
            df = self.extract_transaction_data()
        else:
            raise ValueError(f"Invalid data type: {data_type}")
            
        if self.validate_data(df, validation_rules):
            return df
        else:
            raise ValueError("Data validation failed")
