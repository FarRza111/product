from abc import ABC, abstractmethod
import pandas as pd
from typing import Dict, Any

class TransformationStrategy(ABC):
    """Abstract base class for transformation strategies."""
    
    @abstractmethod
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transform the input data according to the strategy."""
        pass

class StandardizationStrategy(TransformationStrategy):
    """Standardize numeric columns in the dataset."""
    
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        numeric_columns = data.select_dtypes(include=['float64', 'int64']).columns
        result = data.copy()
        
        for column in numeric_columns:
            mean = result[column].mean()
            std = result[column].std()
            result[column] = (result[column] - mean) / std
            
        return result

class AggregationStrategy(TransformationStrategy):
    """Aggregate data based on specified columns and operations."""
    
    def __init__(self, group_by_cols: list, agg_dict: Dict[str, str]):
        self.group_by_cols = group_by_cols
        self.agg_dict = agg_dict
    
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        return data.groupby(self.group_by_cols).agg(self.agg_dict).reset_index()

class EnrichmentStrategy(TransformationStrategy):
    """Enrich data with additional calculated columns."""
    
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        result = data.copy()
        
        # Add derived columns
        if 'quantity' in result.columns and 'price' in result.columns:
            result['total_revenue'] = result['quantity'] * result['price']
        
        if 'date' in result.columns:
            result['date'] = pd.to_datetime(result['date'])
            result['year'] = result['date'].dt.year
            result['month'] = result['date'].dt.month
            result['day_of_week'] = result['date'].dt.day_name()
            
        return result

class DataTransformer:
    """Context class that uses a transformation strategy."""
    
    def __init__(self, strategy: TransformationStrategy):
        self.strategy = strategy
    
    def set_strategy(self, strategy: TransformationStrategy):
        """Change the transformation strategy at runtime."""
        self.strategy = strategy
    
    def transform_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """Execute the current transformation strategy."""
        return self.strategy.transform(data)
