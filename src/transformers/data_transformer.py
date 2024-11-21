import pandas as pd
import numpy as np
from typing import Dict, List
import logging
from datetime import datetime
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BankingDataTransformer:
    def __init__(self, warehouse_connection_string: str = None):
        """Initialize the data transformer."""
        if warehouse_connection_string:
            self.engine = create_engine(warehouse_connection_string)
        else:
            self.engine = None
        
    def transform_customer_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform customer data according to business rules.
        
        Args:
            df: Raw customer DataFrame
            
        Returns:
            Transformed customer DataFrame
        """
        try:
            # Create a copy to avoid modifying the original DataFrame
            transformed_df = df.copy()
            
            # Standardize email addresses
            transformed_df['email'] = transformed_df['email'].str.lower()
            
            # Format phone numbers (assuming standard format)
            transformed_df['phone'] = transformed_df['phone'].str.replace(r'\D', '', regex=True)
            
            # Create full_name column
            transformed_df['full_name'] = transformed_df['first_name'] + ' ' + transformed_df['last_name']
            
            # Convert timestamps to datetime if they aren't already
            transformed_df['created_at'] = pd.to_datetime(transformed_df['created_at'])
            
            # Add ETL timestamp
            transformed_df['etl_timestamp'] = datetime.now()
            
            # Select only the columns in our warehouse schema
            columns = [
                'customer_id',
                'first_name',
                'last_name',
                'full_name',
                'email',
                'phone',
                'created_at',
                'etl_timestamp'
            ]
            transformed_df = transformed_df[columns]
            
            logger.info("Customer data transformation completed successfully")
            return transformed_df
            
        except Exception as e:
            logger.error(f"Error transforming customer data: {str(e)}")
            raise
            
    def transform_transaction_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform transaction data according to business rules.
        
        Args:
            df: Raw transaction DataFrame
            
        Returns:
            Transformed transaction DataFrame
        """
        try:
            # Create a copy to avoid modifying the original DataFrame
            transformed_df = df.copy()
            
            # Get customer keys from dim_customer if engine is available
            if self.engine is not None:
                customer_keys = pd.read_sql(
                    "SELECT customer_id, customer_key FROM dim_customer",
                    self.engine
                )
                transformed_df = transformed_df.merge(
                    customer_keys,
                    on='customer_id',
                    how='left'
                )
            
            # Convert amount to absolute value and add transaction_sign
            transformed_df['transaction_sign'] = np.sign(transformed_df['amount'])
            transformed_df['amount'] = abs(transformed_df['amount'])
            
            # Convert timestamps to datetime if they aren't already
            transformed_df['transaction_date'] = pd.to_datetime(transformed_df['transaction_date'])
            
            # Add transaction categories based on description
            transformed_df['transaction_category'] = transformed_df['description'].apply(self._categorize_transaction)
            
            # Add ETL timestamp
            transformed_df['etl_timestamp'] = datetime.now()
            
            # Select only the columns in our warehouse schema
            columns = [
                'transaction_id',
                'customer_key',
                'amount',
                'transaction_sign',
                'transaction_type',
                'transaction_category',
                'transaction_date',
                'status',
                'description',
                'etl_timestamp'
            ]
            transformed_df = transformed_df[columns]
            
            logger.info("Transaction data transformation completed successfully")
            return transformed_df
            
        except Exception as e:
            logger.error(f"Error transforming transaction data: {str(e)}")
            raise
            
    def _categorize_transaction(self, description: str) -> str:
        """
        Categorize a transaction based on its description.
        """
        description = description.lower()
        
        if any(word in description for word in ['deposit', 'credit']):
            return 'DEPOSIT'
        elif any(word in description for word in ['withdrawal', 'atm']):
            return 'WITHDRAWAL'
        elif 'transfer' in description:
            return 'TRANSFER'
        else:
            return 'OTHER'
