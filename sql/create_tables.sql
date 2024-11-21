-- Drop existing tables if they exist
DROP TABLE IF EXISTS fact_transaction;
DROP TABLE IF EXISTS dim_customer;
DROP TABLE IF EXISTS quality_alerts;
DROP TABLE IF EXISTS data_anomalies;
DROP TABLE IF EXISTS quality_metrics;
DROP TABLE IF EXISTS quality_rules;

-- Create dimension tables
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id INTEGER UNIQUE NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    full_name VARCHAR(200),
    email VARCHAR(255),
    phone VARCHAR(20),
    created_at TIMESTAMP,
    etl_timestamp TIMESTAMP
);

-- Create fact tables
CREATE TABLE IF NOT EXISTS fact_transaction (
    transaction_key SERIAL PRIMARY KEY,
    transaction_id INTEGER UNIQUE NOT NULL,
    customer_key INTEGER REFERENCES dim_customer(customer_key),
    amount DECIMAL(15,2),
    transaction_sign DECIMAL(2,1),
    transaction_type VARCHAR(50),
    transaction_category VARCHAR(50),
    transaction_date TIMESTAMP,
    status VARCHAR(50),
    description TEXT,
    etl_timestamp TIMESTAMP
);

-- Create quality tracking tables
CREATE TABLE IF NOT EXISTS quality_rules (
    rule_id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    column_name VARCHAR(100) NOT NULL,
    rule_type VARCHAR(50) NOT NULL,
    rule_definition JSONB NOT NULL,
    threshold DECIMAL(10,4) NOT NULL,
    severity VARCHAR(20) NOT NULL,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS quality_metrics (
    metric_id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    column_name VARCHAR(100) NOT NULL,
    metric_type VARCHAR(50) NOT NULL,
    metric_value DECIMAL(10,4),
    threshold_value DECIMAL(10,4),
    status VARCHAR(20),
    check_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    details JSONB
);

CREATE TABLE IF NOT EXISTS data_anomalies (
    anomaly_id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    column_name VARCHAR(100) NOT NULL,
    detection_time TIMESTAMP NOT NULL,
    anomaly_type VARCHAR(50) NOT NULL,
    severity VARCHAR(20) NOT NULL,
    affected_rows INTEGER,
    anomaly_score DECIMAL(10,4),
    details JSONB
);

CREATE TABLE IF NOT EXISTS quality_alerts (
    alert_id SERIAL PRIMARY KEY,
    source_type VARCHAR(50) NOT NULL,
    source_id INTEGER NOT NULL,
    alert_time TIMESTAMP NOT NULL,
    severity VARCHAR(20) NOT NULL,
    status VARCHAR(20) DEFAULT 'NEW',
    message TEXT,
    details JSONB
);
