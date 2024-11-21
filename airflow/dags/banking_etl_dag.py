import sys
import os

# Add the project root directory to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
sys.path.append(project_root)

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from config import config

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'banking_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for banking transactions and customer data',
    schedule_interval='0 1 * * *',  # Run daily at 1 AM
    catchup=False
)

# Start task
start_task = DummyOperator(
    task_id='start_pipeline',
    dag=dag
)

# Create tables if they don't exist
create_tables = PostgresOperator(
    task_id='create_tables',
    postgres_conn_id='banking_db',
    sql="""
        CREATE TABLE IF NOT EXISTS dim_customers (
            customer_id SERIAL PRIMARY KEY,
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            email VARCHAR(255),
            phone VARCHAR(20),
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS fact_transactions (
            transaction_id SERIAL PRIMARY KEY,
            customer_id INTEGER REFERENCES dim_customers(customer_id),
            transaction_date TIMESTAMP,
            amount DECIMAL(15,2),
            transaction_type VARCHAR(50),
            status VARCHAR(20),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """,
    dag=dag
)

def extract_customer_data(**context):
    # Implement customer data extraction logic
    pass

def extract_transaction_data(**context):
    # Implement transaction data extraction logic
    pass

def transform_customer_data(**context):
    # Implement customer data transformation logic
    pass

def transform_transaction_data(**context):
    # Implement transaction data transformation logic
    pass

def load_customer_data(**context):
    # Implement customer data loading logic
    pass

def load_transaction_data(**context):
    # Implement transaction data loading logic
    pass

def run_data_quality_checks(**context):
    # Implement data quality checks using Great Expectations
    pass

# Extract tasks
extract_customers = PythonOperator(
    task_id='extract_customers',
    python_callable=extract_customer_data,
    dag=dag
)

extract_transactions = PythonOperator(
    task_id='extract_transactions',
    python_callable=extract_transaction_data,
    dag=dag
)

# Transform tasks
transform_customers = PythonOperator(
    task_id='transform_customers',
    python_callable=transform_customer_data,
    dag=dag
)

transform_transactions = PythonOperator(
    task_id='transform_transactions',
    python_callable=transform_transaction_data,
    dag=dag
)

# Load tasks
load_customers = PythonOperator(
    task_id='load_customers',
    python_callable=load_customer_data,
    dag=dag
)

load_transactions = PythonOperator(
    task_id='load_transactions',
    python_callable=load_transaction_data,
    dag=dag
)

# Data quality task
data_quality_checks = PythonOperator(
    task_id='data_quality_checks',
    python_callable=run_data_quality_checks,
    dag=dag
)

# End task
end_task = DummyOperator(
    task_id='end_pipeline',
    dag=dag
)

# Define task dependencies
start_task >> create_tables
create_tables >> [extract_customers, extract_transactions]
extract_customers >> transform_customers >> load_customers
extract_transactions >> transform_transactions >> load_transactions
[load_customers, load_transactions] >> data_quality_checks >> end_task
