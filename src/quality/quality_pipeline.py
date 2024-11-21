import pandas as pd
import logging
from typing import Dict, List, Optional
from sqlalchemy import create_engine, text
from datetime import datetime

from .validators import DataValidator
from .anomaly_detector import AnomalyDetector

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class QualityPipeline:
    def __init__(self, source_db: str, warehouse_db: str):
        """Initialize the quality pipeline."""
        self.source_engine = create_engine(source_db)
        self.warehouse_engine = create_engine(warehouse_db)
        self.validator = DataValidator(warehouse_db)
        self.anomaly_detector = AnomalyDetector(warehouse_db)
        
    def _load_table_data(self, table_name: str, engine) -> pd.DataFrame:
        """Load data from a table into a DataFrame."""
        query = f"SELECT * FROM {table_name}"
        return pd.read_sql(query, engine)
    
    def process_customer_data(self):
        """Process customer data quality checks."""
        try:
            # Load customer data
            df = self._load_table_data('dim_customer', self.warehouse_engine)
            
            # Run data validation
            self.validator.validate_table(df, 'dim_customer')
            
            # Run anomaly detection
            self.anomaly_detector.analyze_table(
                df,
                'dim_customer',
                numeric_columns=['customer_key'],
                pattern_checks={
                    'email': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
                    'phone': r'^\d{10}$'
                }
            )
            
            logger.info("Completed customer data quality checks")
            
        except Exception as e:
            logger.error(f"Error processing customer quality checks: {str(e)}")
            raise
    
    def process_transaction_data(self):
        """Process transaction data quality checks."""
        try:
            # Load transaction data
            df = self._load_table_data('fact_transaction', self.warehouse_engine)
            
            # Run data validation
            self.validator.validate_table(df, 'fact_transaction')
            
            # Run anomaly detection
            self.anomaly_detector.analyze_table(
                df,
                'fact_transaction',
                numeric_columns=['amount', 'transaction_sign'],
                temporal_analysis={
                    'time_column': 'transaction_date',
                    'value_column': 'amount'
                }
            )
            
            logger.info("Completed transaction data quality checks")
            
        except Exception as e:
            logger.error(f"Error processing transaction quality checks: {str(e)}")
            raise
    
    def initialize_quality_rules(self):
        """Initialize default quality rules."""
        try:
            with self.warehouse_engine.begin() as conn:
                # Customer data rules
                conn.execute(text("""
                    INSERT INTO quality_rules (
                        table_name, column_name, rule_type,
                        rule_definition, threshold, severity
                    ) VALUES
                    ('dim_customer', 'email', 'PATTERN_CHECK',
                     '{"pattern": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+[.][a-zA-Z]{2,}$"}',
                     0.0, 'HIGH'),
                    ('dim_customer', 'phone', 'PATTERN_CHECK',
                     '{"pattern": "^\\\\d{10}$"}',
                     0.0, 'MEDIUM'),
                    ('dim_customer', 'email', 'NULL_CHECK',
                     '{}',
                     0.01, 'HIGH'),
                    ('dim_customer', 'customer_id', 'UNIQUENESS_CHECK',
                     '{}',
                     0.0, 'CRITICAL')
                """))
                
                # Transaction data rules
                conn.execute(text("""
                    INSERT INTO quality_rules (
                        table_name, column_name, rule_type,
                        rule_definition, threshold, severity
                    ) VALUES
                    ('fact_transaction', 'amount', 'RANGE_CHECK',
                     '{"min": 0, "max": 1000000}',
                     0.001, 'HIGH'),
                    ('fact_transaction', 'transaction_date', 'NULL_CHECK',
                     '{}',
                     0.0, 'CRITICAL'),
                    ('fact_transaction', 'customer_key', 'NULL_CHECK',
                     '{}',
                     0.0, 'CRITICAL'),
                    ('fact_transaction', 'transaction_id', 'UNIQUENESS_CHECK',
                     '{}',
                     0.0, 'CRITICAL')
                """))
            
            logger.info("Successfully initialized quality rules")
            
        except Exception as e:
            logger.error(f"Error initializing quality rules: {str(e)}")
            raise
    
    def run_pipeline(self):
        """Run the complete quality pipeline."""
        try:
            logger.info("Starting quality pipeline")
            
            # Initialize rules if needed
            self.initialize_quality_rules()
            
            # Process customer data
            self.process_customer_data()
            
            # Process transaction data
            self.process_transaction_data()
            
            logger.info("Quality pipeline completed successfully")
            
        except Exception as e:
            logger.error(f"Error in quality pipeline: {str(e)}")
            raise
