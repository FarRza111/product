from typing import Optional, List, Dict, Any
from data_source_factory import DataSourceFactory
from transformation_strategy import (
    DataTransformer,
    TransformationStrategy,
    StandardizationStrategy,
    AggregationStrategy,
    EnrichmentStrategy
)
from data_pipeline_decorator import (
    DataPipelineComponent,
    BasicDataPipeline,
    ValidationDecorator,
    LoggingDecorator,
    ProfilingDecorator
)
from pipeline_observer import DataPipeline, PipelineObserver
import pandas as pd
import logging

class DataPipelineBuilder:
    """Builder pattern for constructing complex data pipelines."""
    
    def __init__(self):
        self.reset()
    
    def reset(self):
        """Reset the builder to initial state."""
        self._data_source = None
        self._transformations: List[TransformationStrategy] = []
        self._validators: List[List[str]] = []
        self._observers: List[PipelineObserver] = []
        self._enable_logging = False
        self._enable_profiling = False
        self._logger: Optional[logging.Logger] = None
    
    def set_data_source(self, source_type: str) -> 'DataPipelineBuilder':
        """Set the data source for the pipeline."""
        self._data_source = DataSourceFactory.get_data_source(source_type)
        return self
    
    def add_transformation(self, strategy: TransformationStrategy) -> 'DataPipelineBuilder':
        """Add a transformation strategy to the pipeline."""
        self._transformations.append(strategy)
        return self
    
    def add_validation(self, required_columns: List[str]) -> 'DataPipelineBuilder':
        """Add validation requirements to the pipeline."""
        self._validators.append(required_columns)
        return self
    
    def add_observer(self, observer: PipelineObserver) -> 'DataPipelineBuilder':
        """Add an observer to the pipeline."""
        self._observers.append(observer)
        return self
    
    def enable_logging(self, logger: Optional[logging.Logger] = None) -> 'DataPipelineBuilder':
        """Enable logging for the pipeline."""
        self._enable_logging = True
        self._logger = logger
        return self
    
    def enable_profiling(self) -> 'DataPipelineBuilder':
        """Enable data profiling for the pipeline."""
        self._enable_profiling = True
        return self
    
    def build(self) -> DataPipeline:
        """Build and return the configured pipeline."""
        # Create base pipeline
        pipeline: DataPipelineComponent = BasicDataPipeline()
        
        # Add validators
        for required_columns in self._validators:
            pipeline = ValidationDecorator(pipeline, required_columns)
        
        # Add logging if enabled
        if self._enable_logging:
            pipeline = LoggingDecorator(pipeline, self._logger)
        
        # Add profiling if enabled
        if self._enable_profiling:
            pipeline = ProfilingDecorator(pipeline)
        
        # Create pipeline with observers
        data_pipeline = DataPipeline()
        
        # Add observers
        for observer in self._observers:
            data_pipeline.attach(observer)
        
        return data_pipeline

class DataPipelineDirector:
    """Director class that defines ways to construct the pipeline."""
    
    @staticmethod
    def construct_basic_pipeline(builder: DataPipelineBuilder) -> DataPipeline:
        """Construct a basic pipeline with minimal components."""
        return (builder
                .set_data_source("csv")
                .add_validation(["date", "product_id", "quantity", "price"])
                .enable_logging()
                .build())
    
    @staticmethod
    def construct_full_pipeline(builder: DataPipelineBuilder) -> DataPipeline:
        """Construct a full-featured pipeline with all components."""
        return (builder
                .set_data_source("csv")
                .add_transformation(StandardizationStrategy())
                .add_transformation(EnrichmentStrategy())
                .add_validation(["date", "product_id", "quantity", "price"])
                .enable_logging()
                .enable_profiling()
                .build())
