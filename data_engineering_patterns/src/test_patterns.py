import os
from database_singleton import DatabaseConnection
from data_source_factory import DataSourceFactory
from pipeline_observer import DataPipeline, EmailNotifier, LoggingObserver, MetricsCollector
import pandas as pd

def main():
    # Get the absolute path to the sample data
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    sample_data_path = os.path.join(project_root, 'sample_data', 'sales_data.csv')

    # 1. Create and test data source factory
    print("\n=== Testing Data Source Factory ===")
    csv_source = DataSourceFactory.get_data_source("csv")
    sales_data = csv_source.read_data(sample_data_path)
    print("Sample of sales data:")
    print(sales_data.head(3))

    # 2. Set up pipeline with observers
    print("\n=== Testing Pipeline Observer Pattern ===")
    pipeline = DataPipeline()
    
    # Attach different observers
    pipeline.attach(EmailNotifier())
    pipeline.attach(LoggingObserver())
    metrics_collector = MetricsCollector()
    pipeline.attach(metrics_collector)

    # Process the data
    pipeline.process_data(sales_data)

    # 3. Use database singleton to store processed data
    print("\n=== Testing Database Singleton Pattern ===")
    db = DatabaseConnection()
    
    # Convert DataFrame to SQL
    sales_data.to_sql('sales', db.engine, if_exists='replace', index=False)
    
    # Test query
    result = pd.read_sql("SELECT product_name, SUM(quantity) as total_quantity, SUM(total_amount) as total_revenue FROM sales GROUP BY product_name", db.engine)
    print("\nSales Summary from Database:")
    print(result)

if __name__ == "__main__":
    main()
