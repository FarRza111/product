# Banking Data Engineering Project

A robust ETL pipeline for processing banking transactions and customer data, implementing a modern data warehouse architecture.

## Architecture Overview

```mermaid
graph TD
    subgraph Source Database
        A[Raw Customer Data] --> B[Raw Transaction Data]
    end
    
    subgraph ETL Pipeline
        C[Data Extractor] --> D[Data Transformer]
        D --> E[Data Loader]
    end
    
    subgraph Data Warehouse
        F[dim_customer] --> G[fact_transaction]
    end
    
    A --> C
    B --> C
    E --> F
    E --> G
```

## Project Structure

```
├── src/
│   ├── main.py                # Main ETL pipeline orchestrator
│   ├── init_db.py            # Warehouse schema initialization
│   ├── init_test_data.py     # Test data generator
│   ├── extractors/           # Data extraction modules
│   │   └── data_extractor.py # Source database extraction
│   ├── transformers/         # Data transformation logic
│   │   └── data_transformer.py # Business logic transformations
│   └── loaders/              # Data loading modules
│       └── data_loader.py    # Warehouse loading and indexing
├── sql/
│   └── create_tables.sql     # Warehouse schema definitions
└── .env                      # Configuration and credentials
```

## Implementation Details

### 1. Data Models

#### Source Database Tables
- `customers`: Raw customer information
  - customer_id (PK)
  - first_name
  - last_name
  - email
  - phone
  - address
  - created_at

- `transactions`: Raw transaction records
  - transaction_id (PK)
  - customer_id (FK)
  - amount
  - transaction_type
  - transaction_date
  - status
  - description

#### Data Warehouse Tables
- `dim_customer`: Customer dimension table
  - customer_key (PK)
  - customer_id
  - first_name
  - last_name
  - full_name
  - email
  - phone
  - created_at
  - etl_timestamp

- `fact_transaction`: Transaction fact table
  - transaction_key (PK)
  - transaction_id
  - customer_key (FK)
  - amount
  - transaction_sign
  - transaction_type
  - transaction_category
  - transaction_date
  - status
  - description
  - etl_timestamp

### 2. ETL Components

#### Data Extractor
- Connects to source database
- Extracts customer and transaction data
- Performs initial data validation
- Handles extraction errors and logging

#### Data Transformer
- Standardizes email addresses
- Creates full_name from first_name and last_name
- Determines transaction_sign (+1/-1) from amount
- Categorizes transactions (DEPOSIT, WITHDRAWAL, TRANSFER, OTHER)
- Adds ETL timestamps
- Maps customer keys for fact table

#### Data Loader
- Truncates warehouse tables before loading
- Loads dimension tables first
- Loads fact tables with proper key relationships
- Creates indexes for query optimization
- Handles loading errors and rollbacks

### 3. Configuration

Required environment variables in `.env`:
```
SOURCE_DB_CONNECTION=postgresql://user:pass@host:port/source_db
WAREHOUSE_DB_CONNECTION=postgresql://user:pass@host:port/warehouse_db
```

### 4. Setup Instructions

1. Create and activate virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure databases:
```bash
# Create .env file with database connections
cp .env.example .env
# Edit .env with your database credentials
```

4. Initialize databases:
```bash
# Initialize source database with test data
python src/init_test_data.py

# Initialize warehouse schema
python src/init_db.py
```

5. Run ETL pipeline:
```bash
python src/main.py
```

### 5. Dependencies

- SQLAlchemy (1.4.41+)
- psycopg2
- pandas
- python-dotenv
- typing-extensions (>=4.7.1)

### 6. Error Handling

The pipeline implements comprehensive error handling:
- Database connection errors
- Data validation errors
- Transformation errors
- Loading errors with transaction rollback
- Detailed error logging

### 7. Future Enhancements

- Add incremental loading support
- Implement data quality checks
- Add support for multiple data sources
- Create monitoring dashboard
- Add unit and integration tests
- Implement data lineage tracking
- Add support for real-time processing

## Contributing

1. Create a new branch for each feature
2. Write tests for new features
3. Update documentation
4. Submit pull request

## License

MIT License
