import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database configurations
DATABASE_CONFIG = {
    'source_db': {
        'host': os.getenv('SOURCE_DB_HOST', 'localhost'),
        'port': os.getenv('SOURCE_DB_PORT', '5432'),
        'database': os.getenv('SOURCE_DB_NAME', 'banking_source'),
        'user': os.getenv('SOURCE_DB_USER', 'postgres'),
        'password': os.getenv('SOURCE_DB_PASSWORD', ''),
    },
    'warehouse_db': {
        'host': os.getenv('WAREHOUSE_DB_HOST', 'localhost'),
        'port': os.getenv('WAREHOUSE_DB_PORT', '5432'),
        'database': os.getenv('WAREHOUSE_DB_NAME', 'banking_warehouse'),
        'user': os.getenv('WAREHOUSE_DB_USER', 'postgres'),
        'password': os.getenv('WAREHOUSE_DB_PASSWORD', ''),
    }
}

# Data validation rules
VALIDATION_RULES = {
    'customer_data': {
        'required_columns': ['customer_id', 'first_name', 'last_name', 'email'],
        'data_types': {
            'customer_id': 'int64',
            'first_name': 'object',
            'last_name': 'object',
            'email': 'object'
        }
    },
    'transaction_data': {
        'required_columns': ['transaction_id', 'customer_id', 'amount', 'transaction_date'],
        'data_types': {
            'transaction_id': 'int64',
            'customer_id': 'int64',
            'amount': 'float64',
            'transaction_date': 'datetime64[ns]'
        }
    }
}

# ETL configurations
ETL_CONFIG = {
    'batch_size': 1000,
    'max_retries': 3,
    'retry_delay': 300,  # seconds
    'archive_after_days': 365,  # archive data older than 1 year
}

# Logging configuration
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
        },
    },
    'handlers': {
        'default': {
            'level': 'INFO',
            'formatter': 'standard',
            'class': 'logging.StreamHandler',
        },
        'file': {
            'level': 'INFO',
            'formatter': 'standard',
            'class': 'logging.FileHandler',
            'filename': 'logs/etl.log',
            'mode': 'a',
        },
    },
    'loggers': {
        '': {  # root logger
            'handlers': ['default', 'file'],
            'level': 'INFO',
            'propagate': True
        }
    }
}

# Data transformation rules
TRANSFORMATION_RULES = {
    'customer_data': {
        'email_standardization': True,
        'phone_standardization': True,
        'create_full_name': True,
    },
    'transaction_data': {
        'amount_categories': {
            'Very Small': (0, 100),
            'Small': (100, 500),
            'Medium': (500, 1000),
            'Large': (1000, 5000),
            'Very Large': (5000, float('inf'))
        },
        'calculate_running_balance': True,
        'add_time_dimensions': True,
    }
}

# Data quality thresholds
DATA_QUALITY_THRESHOLDS = {
    'max_null_percentage': 0.05,  # Maximum allowed percentage of null values
    'min_daily_transactions': 100,  # Minimum expected daily transactions
    'max_amount': 1000000,  # Maximum allowed transaction amount
    'min_amount': -1000000,  # Minimum allowed transaction amount
}

# Create connection strings
def get_db_connection_string(db_config):
    return f"postgresql://{db_config['user']}:{db_config['password']}@" \
           f"{db_config['host']}:{db_config['port']}/{db_config['database']}"

SOURCE_DB_CONNECTION = get_db_connection_string(DATABASE_CONFIG['source_db'])
WAREHOUSE_DB_CONNECTION = get_db_connection_string(DATABASE_CONFIG['warehouse_db'])
