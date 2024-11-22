import os
import logging
import pandas as pd
from database_singleton import DatabaseConnection
from data_source_factory import DataSourceFactory
from pipeline_observer import EmailNotifier, LoggingObserver, MetricsCollector
from transformation_strategy import (
    DataTransformer,
    StandardizationStrategy,
    AggregationStrategy,
    EnrichmentStrategy
)
from data_pipeline_decorator import (
    BasicDataPipeline,
    ValidationDecorator,
    LoggingDecorator,
    ProfilingDecorator
)
from pipeline_builder import DataPipelineBuilder, DataPipelineDirector

def setup_logging():
    """Set up logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def test_factory_pattern(data_path: str):
    """Test the Factory pattern for data sources."""
    print("\n=== Testing Factory Pattern ===")
    csv_source = DataSourceFactory.get_data_source("csv")
    data = csv_source.read_data(data_path)
    print("Sample of loaded data:")
    print(data.head(3))
    return data

def test_strategy_pattern(data: pd.DataFrame):
    """Test the Strategy pattern for data transformations."""
    print("\n=== Testing Strategy Pattern ===")
    
    # Test standardization strategy
    print("\nApplying Standardization Strategy:")
    transformer = DataTransformer(StandardizationStrategy())
    standardized_data = transformer.transform_data(data)
    print(standardized_data.head(3))
    
    # Test aggregation strategy
    print("\nApplying Aggregation Strategy:")
    transformer.set_strategy(AggregationStrategy(
        group_by_cols=['product_name'],
        agg_dict={'quantity': 'sum', 'total_amount': 'sum'}
    ))
    aggregated_data = transformer.transform_data(data)
    print(aggregated_data.head(3))
    
    # Test enrichment strategy
    print("\nApplying Enrichment Strategy:")
    transformer.set_strategy(EnrichmentStrategy())
    enriched_data = transformer.transform_data(data)
    print(enriched_data.head(3))
    
    return enriched_data

def test_decorator_pattern(data: pd.DataFrame):
    """Test the Decorator pattern for pipeline components."""
    print("\n=== Testing Decorator Pattern ===")
    
    # Create a pipeline with multiple decorators
    pipeline = BasicDataPipeline()
    pipeline = ValidationDecorator(pipeline, ["date", "product_id", "quantity", "price"])
    pipeline = LoggingDecorator(pipeline)
    pipeline = ProfilingDecorator(pipeline)
    
    # Process data through the decorated pipeline
    result = pipeline.process(data)
    return result

def test_builder_pattern():
    """Test the Builder pattern for pipeline construction."""
    print("\n=== Testing Builder Pattern ===")
    
    # Create builder
    builder = DataPipelineBuilder()
    
    # Test basic pipeline construction
    print("\nConstructing Basic Pipeline:")
    basic_pipeline = DataPipelineDirector.construct_basic_pipeline(builder)
    print("Basic pipeline constructed successfully")
    
    # Reset builder and test full pipeline construction
    builder.reset()
    print("\nConstructing Full Pipeline:")
    full_pipeline = DataPipelineDirector.construct_full_pipeline(builder)
    print("Full pipeline constructed successfully")
    
    return full_pipeline

def main():
    # Setup
    logger = setup_logging()
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    sample_data_path = os.path.join(project_root, 'sample_data', 'sales_data.csv')
    
    try:
        # Test Factory Pattern
        data = test_factory_pattern(sample_data_path)
        
        # Test Strategy Pattern
        transformed_data = test_strategy_pattern(data)
        
        # Test Decorator Pattern
        processed_data = test_decorator_pattern(transformed_data)
        
        # Test Builder Pattern
        pipeline = test_builder_pattern()
        
        # Store results in database using Singleton Pattern
        print("\n=== Testing Singleton Pattern ===")
        db = DatabaseConnection()
        processed_data.to_sql('processed_sales', db.engine, if_exists='replace', index=False)
        print("Data successfully stored in database")
        
        # Query and display results
        result = pd.read_sql(
            """
            SELECT 
                product_name,
                SUM(quantity) as total_quantity,
                SUM(total_amount) as total_revenue
            FROM processed_sales
            GROUP BY product_name
            """,
            db.engine
        )
        print("\nFinal Results from Database:")
        print(result)
        
    except Exception as e:
        logger.error(f"Error during pattern testing: {str(e)}")
        raise

if __name__ == "__main__":
    main()
