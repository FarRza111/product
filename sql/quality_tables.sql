-- Drop existing tables if they exist
DROP TABLE IF EXISTS quality_metrics CASCADE;
DROP TABLE IF EXISTS data_anomalies CASCADE;
DROP TABLE IF EXISTS quality_rules CASCADE;
DROP TABLE IF EXISTS quality_alerts CASCADE;

-- Quality Metrics Table
CREATE TABLE IF NOT EXISTS quality_metrics (
    metric_id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    column_name VARCHAR(100),
    metric_type VARCHAR(50) NOT NULL,
    metric_value DECIMAL(10,4),
    threshold_value DECIMAL(10,4),
    check_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) CHECK (status IN ('PASSED', 'FAILED', 'WARNING'))
);

-- Data Anomalies Table
CREATE TABLE IF NOT EXISTS data_anomalies (
    anomaly_id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    record_id VARCHAR(100),
    anomaly_type VARCHAR(50) NOT NULL,
    description TEXT,
    severity VARCHAR(20) CHECK (severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    resolved_at TIMESTAMP,
    status VARCHAR(20) DEFAULT 'OPEN' CHECK (status IN ('OPEN', 'IN_PROGRESS', 'RESOLVED'))
);

-- Quality Rules Table
CREATE TABLE IF NOT EXISTS quality_rules (
    rule_id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    column_name VARCHAR(100),
    rule_type VARCHAR(50) NOT NULL,
    rule_definition JSONB NOT NULL,
    threshold DECIMAL(10,4),
    severity VARCHAR(20) CHECK (severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Quality Alerts Table
CREATE TABLE IF NOT EXISTS quality_alerts (
    alert_id SERIAL PRIMARY KEY,
    rule_id INTEGER REFERENCES quality_rules(rule_id),
    metric_id INTEGER REFERENCES quality_metrics(metric_id),
    alert_type VARCHAR(50) NOT NULL,
    description TEXT,
    severity VARCHAR(20) CHECK (severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    acknowledged_at TIMESTAMP,
    resolved_at TIMESTAMP,
    status VARCHAR(20) DEFAULT 'OPEN' CHECK (status IN ('OPEN', 'ACKNOWLEDGED', 'RESOLVED'))
);

-- Create indexes
CREATE INDEX idx_metrics_table ON quality_metrics(table_name);
CREATE INDEX idx_metrics_timestamp ON quality_metrics(check_timestamp);
CREATE INDEX idx_anomalies_table ON data_anomalies(table_name);
CREATE INDEX idx_anomalies_status ON data_anomalies(status);
CREATE INDEX idx_rules_table ON quality_rules(table_name);
CREATE INDEX idx_alerts_status ON quality_alerts(status);
