import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import logging
from sqlalchemy import create_engine, text
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AnomalyDetector:
    def __init__(self, db_connection: str):
        """Initialize the anomaly detector with database connection."""
        self.engine = create_engine(db_connection)
        self.scaler = StandardScaler()
        
    def detect_statistical_anomalies(self, df: pd.DataFrame, table_name: str, 
                                   numeric_columns: List[str], contamination: float = 0.1) -> List[Dict]:
        """
        Detect statistical anomalies using Isolation Forest.
        
        Args:
            df: Input DataFrame
            table_name: Name of the table being analyzed
            numeric_columns: List of numeric columns to analyze
            contamination: Expected proportion of outliers
            
        Returns:
            List of anomaly records
        """
        try:
            # Select only numeric columns
            numeric_data = df[numeric_columns]
            
            # Scale the data
            scaled_data = self.scaler.fit_transform(numeric_data)
            
            # Train Isolation Forest
            iso_forest = IsolationForest(
                contamination=contamination,
                random_state=42
            )
            
            # Predict anomalies (-1 for anomalies, 1 for normal)
            predictions = iso_forest.fit_predict(scaled_data)
            
            # Get anomaly scores
            scores = iso_forest.score_samples(scaled_data)
            
            anomalies = []
            for idx, is_anomaly in enumerate(predictions == -1):
                if is_anomaly:
                    # Get the anomalous values
                    values = numeric_data.iloc[idx]
                    
                    # Create description
                    description = "Statistical anomaly detected: "
                    description += ", ".join([
                        f"{col}: {values[col]:.2f}"
                        for col in numeric_columns
                    ])
                    
                    # Determine severity based on anomaly score
                    score = abs(scores[idx])
                    if score > 0.8:
                        severity = 'CRITICAL'
                    elif score > 0.6:
                        severity = 'HIGH'
                    elif score > 0.4:
                        severity = 'MEDIUM'
                    else:
                        severity = 'LOW'
                    
                    anomalies.append({
                        'table_name': table_name,
                        'record_id': str(df.index[idx]),
                        'anomaly_type': 'STATISTICAL',
                        'description': description,
                        'severity': severity
                    })
            
            return anomalies
            
        except Exception as e:
            logger.error(f"Error detecting statistical anomalies: {str(e)}")
            raise
    
    def detect_temporal_anomalies(self, df: pd.DataFrame, table_name: str,
                                time_column: str, value_column: str,
                                window_size: int = 24) -> List[Dict]:
        """
        Detect temporal anomalies using moving average.
        
        Args:
            df: Input DataFrame
            table_name: Name of the table being analyzed
            time_column: Name of the timestamp column
            value_column: Name of the value column to analyze
            window_size: Size of the moving window
            
        Returns:
            List of anomaly records
        """
        try:
            # Sort by time
            df = df.sort_values(time_column)
            
            # Calculate moving average and standard deviation
            rolling_mean = df[value_column].rolling(window=window_size).mean()
            rolling_std = df[value_column].rolling(window=window_size).std()
            
            # Define bounds
            upper_bound = rolling_mean + 3 * rolling_std
            lower_bound = rolling_mean - 3 * rolling_std
            
            anomalies = []
            for idx in range(len(df)):
                value = df[value_column].iloc[idx]
                if value > upper_bound.iloc[idx] or value < lower_bound.iloc[idx]:
                    # Calculate deviation
                    deviation = abs(value - rolling_mean.iloc[idx]) / rolling_std.iloc[idx]
                    
                    # Determine severity based on deviation
                    if deviation > 5:
                        severity = 'CRITICAL'
                    elif deviation > 4:
                        severity = 'HIGH'
                    elif deviation > 3:
                        severity = 'MEDIUM'
                    else:
                        severity = 'LOW'
                    
                    description = (
                        f"Temporal anomaly detected: {value_column}={value:.2f}, "
                        f"Expected range: [{lower_bound.iloc[idx]:.2f}, {upper_bound.iloc[idx]:.2f}]"
                    )
                    
                    anomalies.append({
                        'table_name': table_name,
                        'record_id': str(df.index[idx]),
                        'anomaly_type': 'TEMPORAL',
                        'description': description,
                        'severity': severity
                    })
            
            return anomalies
            
        except Exception as e:
            logger.error(f"Error detecting temporal anomalies: {str(e)}")
            raise
    
    def detect_pattern_anomalies(self, df: pd.DataFrame, table_name: str,
                               column_patterns: Dict[str, str]) -> List[Dict]:
        """
        Detect pattern anomalies using regex patterns.
        
        Args:
            df: Input DataFrame
            table_name: Name of the table being analyzed
            column_patterns: Dictionary of column names and their expected regex patterns
            
        Returns:
            List of anomaly records
        """
        try:
            anomalies = []
            for column, pattern in column_patterns.items():
                if column in df.columns:
                    # Check pattern matches
                    mask = ~df[column].str.match(pattern)
                    invalid_values = df[mask]
                    
                    for idx, value in invalid_values.items():
                        description = f"Pattern mismatch in column {column}: {value}"
                        
                        anomalies.append({
                            'table_name': table_name,
                            'record_id': str(idx),
                            'anomaly_type': 'PATTERN',
                            'description': description,
                            'severity': 'MEDIUM'
                        })
            
            return anomalies
            
        except Exception as e:
            logger.error(f"Error detecting pattern anomalies: {str(e)}")
            raise
    
    def save_anomalies(self, anomalies: List[Dict]):
        """Save detected anomalies to the database."""
        if not anomalies:
            return
            
        query = """
            INSERT INTO data_anomalies (
                table_name, record_id, anomaly_type,
                description, severity
            )
            VALUES (
                :table_name, :record_id, :anomaly_type,
                :description, :severity
            )
        """
        
        with self.engine.begin() as conn:
            for anomaly in anomalies:
                conn.execute(text(query), anomaly)
    
    def analyze_table(self, df: pd.DataFrame, table_name: str,
                     numeric_columns: Optional[List[str]] = None,
                     temporal_analysis: Optional[Dict[str, str]] = None,
                     pattern_checks: Optional[Dict[str, str]] = None):
        """
        Run all anomaly detection methods for a table.
        
        Args:
            df: Input DataFrame
            table_name: Name of the table being analyzed
            numeric_columns: List of numeric columns for statistical analysis
            temporal_analysis: Dict with time_column and value_column for temporal analysis
            pattern_checks: Dict of column names and their expected patterns
        """
        logger.info(f"Starting anomaly detection for table: {table_name}")
        
        anomalies = []
        
        # Statistical anomalies
        if numeric_columns:
            statistical_anomalies = self.detect_statistical_anomalies(
                df, table_name, numeric_columns
            )
            anomalies.extend(statistical_anomalies)
        
        # Temporal anomalies
        if temporal_analysis:
            temporal_anomalies = self.detect_temporal_anomalies(
                df,
                table_name,
                temporal_analysis['time_column'],
                temporal_analysis['value_column']
            )
            anomalies.extend(temporal_anomalies)
        
        # Pattern anomalies
        if pattern_checks:
            pattern_anomalies = self.detect_pattern_anomalies(
                df, table_name, pattern_checks
            )
            anomalies.extend(pattern_anomalies)
        
        # Save all detected anomalies
        self.save_anomalies(anomalies)
        
        logger.info(f"Completed anomaly detection for table: {table_name}")
        return anomalies
