from abc import ABC, abstractmethod
import pandas as pd
from datetime import datetime
import logging
from typing import Optional

class DataPipelineComponent(ABC):
    """Abstract base class for the data pipeline components."""
    
    @abstractmethod
    def process(self, data: pd.DataFrame) -> pd.DataFrame:
        """Process the input data."""
        pass

class BasicDataPipeline(DataPipelineComponent):
    """Basic data pipeline that performs the core transformation."""
    
    def process(self, data: pd.DataFrame) -> pd.DataFrame:
        # Simulate some basic data processing
        return data.copy()

class DataPipelineDecorator(DataPipelineComponent):
    """Base decorator class for data pipeline components."""
    
    def __init__(self, pipeline: DataPipelineComponent):
        self._pipeline = pipeline

    def process(self, data: pd.DataFrame) -> pd.DataFrame:
        return self._pipeline.process(data)

class ValidationDecorator(DataPipelineDecorator):
    """Decorator that adds data validation capabilities."""
    
    def __init__(self, pipeline: DataPipelineComponent, required_columns: Optional[list] = None):
        super().__init__(pipeline)
        self.required_columns = required_columns or []
    
    def process(self, data: pd.DataFrame) -> pd.DataFrame:
        self._validate_data(data)
        return super().process(data)
    
    def _validate_data(self, data: pd.DataFrame):
        """Validate the input data."""
        # Check for required columns
        missing_columns = [col for col in self.required_columns if col not in data.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Check for null values
        null_counts = data[self.required_columns].isnull().sum()
        if null_counts.any():
            raise ValueError(f"Null values found in columns: {null_counts[null_counts > 0].to_dict()}")

class LoggingDecorator(DataPipelineDecorator):
    """Decorator that adds logging capabilities."""
    
    def __init__(self, pipeline: DataPipelineComponent, logger: Optional[logging.Logger] = None):
        super().__init__(pipeline)
        self.logger = logger or logging.getLogger(__name__)
    
    def process(self, data: pd.DataFrame) -> pd.DataFrame:
        start_time = datetime.now()
        self.logger.info(f"Starting data processing with shape: {data.shape}")
        
        try:
            result = super().process(data)
            self.logger.info(f"Completed data processing. Output shape: {result.shape}")
            self.logger.info(f"Processing time: {datetime.now() - start_time}")
            return result
        except Exception as e:
            self.logger.error(f"Error during data processing: {str(e)}")
            raise

class ProfilingDecorator(DataPipelineDecorator):
    """Decorator that adds data profiling capabilities."""
    
    def process(self, data: pd.DataFrame) -> pd.DataFrame:
        # Profile input data
        self._profile_data("Input", data)
        
        # Process data
        result = super().process(data)
        
        # Profile output data
        self._profile_data("Output", result)
        
        return result
    
    def _profile_data(self, stage: str, data: pd.DataFrame):
        """Generate basic data profile."""
        print(f"\n{stage} Data Profile:")
        print(f"Shape: {data.shape}")
        print("\nData Types:")
        print(data.dtypes)
        print("\nMissing Values:")
        print(data.isnull().sum())
        print("\nNumeric Columns Summary:")
        print(data.describe().round(2))
