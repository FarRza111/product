import pandas as pd
import numpy as np
from typing import Dict, List, Optional
from datetime import datetime
import logging
from sqlalchemy import create_engine, text
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataValidator:
    def __init__(self, db_connection: str):
        """Initialize the data validator with database connection."""
        self.engine = create_engine(db_connection)
        
    def load_rules(self) -> List[Dict]:
        """Load active quality rules from the database."""
        query = """
            SELECT rule_id, table_name, column_name, rule_type, 
                   rule_definition, threshold, severity
            FROM quality_rules
            WHERE is_active = true
        """
        with self.engine.connect() as conn:
            result = conn.execute(text(query))
            return [dict(row) for row in result]
            
    def validate_nulls(self, df: pd.DataFrame, table_name: str, rules: List[Dict]) -> List[Dict]:
        """Check for null values in specified columns."""
        metrics = []
        for rule in rules:
            if rule['rule_type'] == 'NULL_CHECK' and rule['table_name'] == table_name:
                column = rule['column_name']
                if column in df.columns:
                    null_ratio = df[column].isnull().mean()
                    threshold = float(rule['threshold'])
                    status = 'PASSED' if null_ratio <= threshold else 'FAILED'
                    
                    metrics.append({
                        'table_name': table_name,
                        'column_name': column,
                        'metric_type': 'NULL_RATIO',
                        'metric_value': float(null_ratio),
                        'threshold_value': threshold,
                        'status': status
                    })
                    
                    if status == 'FAILED':
                        self._create_anomaly(
                            table_name=table_name,
                            anomaly_type='HIGH_NULL_RATIO',
                            description=f'Column {column} has {null_ratio:.2%} null values',
                            severity=rule['severity']
                        )
        return metrics
    
    def validate_uniqueness(self, df: pd.DataFrame, table_name: str, rules: List[Dict]) -> List[Dict]:
        """Check for duplicate values in specified columns."""
        metrics = []
        for rule in rules:
            if rule['rule_type'] == 'UNIQUENESS_CHECK' and rule['table_name'] == table_name:
                column = rule['column_name']
                if column in df.columns:
                    duplicate_ratio = 1 - df[column].nunique() / len(df)
                    threshold = float(rule['threshold'])
                    status = 'PASSED' if duplicate_ratio <= threshold else 'FAILED'
                    
                    metrics.append({
                        'table_name': table_name,
                        'column_name': column,
                        'metric_type': 'DUPLICATE_RATIO',
                        'metric_value': float(duplicate_ratio),
                        'threshold_value': threshold,
                        'status': status
                    })
                    
                    if status == 'FAILED':
                        self._create_anomaly(
                            table_name=table_name,
                            anomaly_type='HIGH_DUPLICATE_RATIO',
                            description=f'Column {column} has {duplicate_ratio:.2%} duplicate values',
                            severity=rule['severity']
                        )
        return metrics
    
    def validate_ranges(self, df: pd.DataFrame, table_name: str, rules: List[Dict]) -> List[Dict]:
        """Check if values are within specified ranges."""
        metrics = []
        for rule in rules:
            if rule['rule_type'] == 'RANGE_CHECK' and rule['table_name'] == table_name:
                column = rule['column_name']
                if column in df.columns:
                    rule_def = rule['rule_definition']
                    min_val = float(rule_def.get('min', float('-inf')))
                    max_val = float(rule_def.get('max', float('inf')))
                    
                    out_of_range_ratio = ((df[column] < min_val) | (df[column] > max_val)).mean()
                    threshold = float(rule['threshold'])
                    status = 'PASSED' if out_of_range_ratio <= threshold else 'FAILED'
                    
                    metrics.append({
                        'table_name': table_name,
                        'column_name': column,
                        'metric_type': 'OUT_OF_RANGE_RATIO',
                        'metric_value': float(out_of_range_ratio),
                        'threshold_value': threshold,
                        'status': status
                    })
                    
                    if status == 'FAILED':
                        self._create_anomaly(
                            table_name=table_name,
                            anomaly_type='OUT_OF_RANGE_VALUES',
                            description=f'Column {column} has {out_of_range_ratio:.2%} values outside range [{min_val}, {max_val}]',
                            severity=rule['severity']
                        )
        return metrics
    
    def validate_patterns(self, df: pd.DataFrame, table_name: str, rules: List[Dict]) -> List[Dict]:
        """Check if values match specified patterns."""
        metrics = []
        for rule in rules:
            if rule['rule_type'] == 'PATTERN_CHECK' and rule['table_name'] == table_name:
                column = rule['column_name']
                if column in df.columns:
                    pattern = rule['rule_definition'].get('pattern')
                    if pattern:
                        # Convert pattern check to boolean mask
                        invalid_ratio = 1 - df[column].astype(str).str.match(pattern).mean()
                        threshold = float(rule['threshold'])
                        status = 'PASSED' if invalid_ratio <= threshold else 'FAILED'
                        
                        metrics.append({
                            'table_name': table_name,
                            'column_name': column,
                            'metric_type': 'INVALID_PATTERN_RATIO',
                            'metric_value': float(invalid_ratio),
                            'threshold_value': threshold,
                            'status': status
                        })
                        
                        if status == 'FAILED':
                            self._create_anomaly(
                                table_name=table_name,
                                anomaly_type='INVALID_PATTERNS',
                                description=f'Column {column} has {invalid_ratio:.2%} values not matching pattern',
                                severity=rule['severity']
                            )
        return metrics
    
    def _create_anomaly(self, table_name: str, anomaly_type: str, description: str, severity: str):
        """Create a new anomaly record."""
        query = """
            INSERT INTO data_anomalies (
                table_name, column_name, anomaly_type, 
                detection_time, severity, details
            )
            VALUES (
                :table_name, :column_name, :anomaly_type,
                CURRENT_TIMESTAMP, :severity, :details
            )
        """
        with self.engine.begin() as conn:
            conn.execute(
                text(query),
                {
                    'table_name': table_name,
                    'column_name': table_name.split('.')[-1],
                    'anomaly_type': anomaly_type,
                    'severity': severity,
                    'details': json.dumps({'description': description})
                }
            )
    
    def save_metrics(self, metrics: List[Dict]):
        """Save quality metrics to the database."""
        if not metrics:
            return
            
        query = """
            INSERT INTO quality_metrics (
                table_name, column_name, metric_type, 
                metric_value, threshold_value, status
            )
            VALUES (
                :table_name, :column_name, :metric_type,
                :metric_value, :threshold_value, :status
            )
        """
        with self.engine.begin() as conn:
            for metric in metrics:
                conn.execute(text(query), metric)
    
    def validate_table(self, df: pd.DataFrame, table_name: str):
        """Run all validations for a table."""
        logger.info(f"Starting validation for table: {table_name}")
        
        # Load quality rules
        rules = self.load_rules()
        
        # Run all validations
        metrics = []
        metrics.extend(self.validate_nulls(df, table_name, rules))
        metrics.extend(self.validate_uniqueness(df, table_name, rules))
        metrics.extend(self.validate_ranges(df, table_name, rules))
        metrics.extend(self.validate_patterns(df, table_name, rules))
        
        # Save metrics
        self.save_metrics(metrics)
        
        logger.info(f"Completed validation for table: {table_name}")
        return metrics
