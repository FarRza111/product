from abc import ABC, abstractmethod
import pandas as pd
from pyspark.sql import SparkSession

class DataSource(ABC):
    """Abstract base class for different data sources"""
    
    @abstractmethod
    def read_data(self, source_path: str):
        pass

class CSVDataSource(DataSource):
    """Concrete implementation for CSV data source"""
    
    def read_data(self, source_path: str):
        return pd.read_csv(source_path)

class ParquetDataSource(DataSource):
    """Concrete implementation for Parquet data source"""
    
    def read_data(self, source_path: str):
        return pd.read_parquet(source_path)

class SparkDataSource(DataSource):
    """Concrete implementation for Spark data source"""
    
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("DataEngineering") \
            .getOrCreate()

    def read_data(self, source_path: str):
        return self.spark.read.parquet(source_path)

class DataSourceFactory:
    """Factory class for creating different data sources"""
    
    @staticmethod
    def get_data_source(source_type: str) -> DataSource:
        if source_type.lower() == "csv":
            return CSVDataSource()
        elif source_type.lower() == "parquet":
            return ParquetDataSource()
        elif source_type.lower() == "spark":
            return SparkDataSource()
        else:
            raise ValueError(f"Unsupported data source type: {source_type}")

# Usage example
if __name__ == "__main__":
    # Create different data sources using the factory
    csv_source = DataSourceFactory.get_data_source("csv")
    parquet_source = DataSourceFactory.get_data_source("parquet")
    
    # Read data using the created sources
    csv_data = csv_source.read_data("example.csv")
    parquet_data = parquet_source.read_data("example.parquet")
