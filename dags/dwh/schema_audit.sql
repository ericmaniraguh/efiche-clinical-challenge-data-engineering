-- ============================================================================
-- eFICHE Analytics Audit Schema
-- ============================================================================

-- CREATE AUDIT SCHEMA
CREATE SCHEMA IF NOT EXISTS audit;

-- ============================================================================
-- AUDIT TABLES
-- ============================================================================

-- Data Quality Log
CREATE TABLE IF NOT EXISTS audit.data_quality_log (
    log_id SERIAL PRIMARY KEY,
    schema_name VARCHAR(50),
    table_name VARCHAR(100),
    issue_type VARCHAR(50),
    issue_description TEXT,
    severity VARCHAR(20),
    is_synthetic_data BOOLEAN DEFAULT false,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dq_log_table ON audit.data_quality_log(table_name);
CREATE INDEX IF NOT EXISTS idx_dq_log_created ON audit.data_quality_log(created_at);
CREATE INDEX IF NOT EXISTS idx_dq_log_severity ON audit.data_quality_log(severity);

SELECT 'Audit schema and tables created successfully!' as status;
