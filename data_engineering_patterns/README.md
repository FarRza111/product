# Data Engineering Design Patterns

This project demonstrates the implementation of common design patterns in data engineering using Python. It showcases six fundamental patterns: Factory, Observer, Singleton, Strategy, Decorator, and Builder, applied to data processing workflows.

## Design Patterns Implemented

### 1. Data Source Factory Pattern
- Implements a factory method for creating different data source readers
- Currently supports CSV file reading
- Easily extensible for other data sources (JSON, XML, etc.)

### 2. Pipeline Observer Pattern
- Implements a data pipeline with multiple observers
- Includes three types of observers:
  - Email Notifier: Simulates sending email notifications
  - Logging Observer: Records pipeline events
  - Metrics Collector: Collects pipeline metrics

### 3. Database Singleton Pattern
- Ensures a single database connection throughout the application
- Implements thread-safe database operations
- Uses SQLite for demonstration purposes

### 4. Strategy Pattern
- Implements different data transformation strategies
- Includes three strategies:
  - Standardization: Standardizes numeric columns
  - Aggregation: Groups and aggregates data
  - Enrichment: Adds derived columns and features

### 5. Decorator Pattern
- Adds capabilities to data pipelines without altering their core functionality
- Implements three decorators:
  - Validation: Ensures data quality and completeness
  - Logging: Adds detailed logging of pipeline operations
  - Profiling: Generates data profiles before and after processing

### 6. Builder Pattern
- Constructs complex data pipelines with various configurations
- Provides a fluent interface for pipeline construction
- Includes a director class with predefined pipeline configurations

## Project Structure

```
data_engineering_patterns/
├── src/
│   ├── data_source_factory.py     # Factory pattern implementation
│   ├── database_singleton.py      # Singleton pattern implementation
│   ├── pipeline_observer.py       # Observer pattern implementation
│   ├── transformation_strategy.py # Strategy pattern implementation
│   ├── data_pipeline_decorator.py # Decorator pattern implementation
│   ├── pipeline_builder.py        # Builder pattern implementation
│   ├── test_patterns.py          # Basic pattern demonstration
│   └── test_all_patterns.py      # Comprehensive pattern demonstration
├── sample_data/
│   └── sales_data.csv            # Sample data for testing
└── requirements.txt              # Project dependencies
```

## Requirements

- Python 3.x
- Dependencies listed in `requirements.txt`

## Installation

1. Clone the repository
2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Running the Demo

To run the basic pattern demonstration:
```bash
python src/test_patterns.py
```

To run the comprehensive demonstration of all patterns:
```bash
python src/test_all_patterns.py
```

This will demonstrate:
1. Loading data using the Factory pattern
2. Transforming data using various Strategy patterns
3. Processing data through decorated pipelines
4. Building complex pipelines using the Builder pattern
5. Storing and querying data using the Singleton pattern

## Output Example

The script will output:
- Sample of the loaded sales data
- Pipeline processing notifications
- Database query results showing sales summaries
- Data transformation results using different strategies
- Decorated pipeline operations
- Complex pipeline configurations built using the Builder pattern

## Extending the Patterns

### Adding New Data Sources
Extend the `DataSourceFactory` class in `data_source_factory.py` with new source types.

### Adding New Observers
Create new observer classes that inherit from the base observer in `pipeline_observer.py`.

### Modifying Database Operations
Modify the `DatabaseConnection` class in `database_singleton.py` to add new database operations.

### Adding New Strategies
Create new strategy classes that implement the `TransformationStrategy` interface in `transformation_strategy.py`.

### Adding New Decorators
Create new decorator classes that implement the `PipelineDecorator` interface in `data_pipeline_decorator.py`.

### Adding New Pipeline Configurations
Create new pipeline configurations using the `PipelineBuilder` class in `pipeline_builder.py`.
