-- ============================================================================
-- eFICHE Analytics Schema - CORRECTED VERSION
-- ============================================================================
-- CREATE ANALYTICS SCHEMA IN efiche_clinical_database
-- ============================================================================


-- ============================================================================
-- DIMENSION TABLES
-- ============================================================================

-- Patient Dimension
CREATE TABLE IF NOT EXISTS analytics.dim_patient (
    patient_sk SERIAL PRIMARY KEY,
    patient_id UUID NOT NULL UNIQUE,
    patient_code VARCHAR(50),
    sex VARCHAR(10),
    date_of_birth DATE,
    age INT,
    geographic_location VARCHAR(255),
    first_encounter_date TIMESTAMP,
    last_encounter_date TIMESTAMP,
    total_encounters INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT true
);

CREATE INDEX IF NOT EXISTS idx_dim_patient_id ON analytics.dim_patient(patient_id);
CREATE INDEX IF NOT EXISTS idx_dim_patient_code ON analytics.dim_patient(patient_code);
CREATE INDEX IF NOT EXISTS idx_dim_patient_location ON analytics.dim_patient(geographic_location);

-- Facility Dimension
CREATE TABLE IF NOT EXISTS analytics.dim_facility (
    facility_sk SERIAL PRIMARY KEY,
    facility_id UUID NOT NULL UNIQUE,
    facility_code VARCHAR(50),
    facility_name VARCHAR(255),
    location VARCHAR(255),
    region VARCHAR(100),
    country VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT true
);

CREATE INDEX IF NOT EXISTS idx_dim_facility_id ON analytics.dim_facility(facility_id);
CREATE INDEX IF NOT EXISTS idx_dim_facility_code ON analytics.dim_facility(facility_code);
CREATE INDEX IF NOT EXISTS idx_dim_facility_location ON analytics.dim_facility(location);

-- Modality Dimension
CREATE TABLE IF NOT EXISTS analytics.dim_modality (
    modality_sk SERIAL PRIMARY KEY,
    modality_id UUID NOT NULL UNIQUE,
    modality_code VARCHAR(50),
    modality_name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT true
);

CREATE INDEX IF NOT EXISTS idx_dim_modality_id ON analytics.dim_modality(modality_id);
CREATE INDEX IF NOT EXISTS idx_dim_modality_code ON analytics.dim_modality(modality_code);

-- Diagnosis Dimension
CREATE TABLE IF NOT EXISTS analytics.dim_diagnosis (
    diagnosis_sk SERIAL PRIMARY KEY,
    diagnosis_id UUID NOT NULL UNIQUE,
    diagnosis_code VARCHAR(50),
    diagnosis_name VARCHAR(255),
    category VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT true
);

CREATE INDEX IF NOT EXISTS idx_dim_diagnosis_id ON analytics.dim_diagnosis(diagnosis_id);
CREATE INDEX IF NOT EXISTS idx_dim_diagnosis_code ON analytics.dim_diagnosis(diagnosis_code);
CREATE INDEX IF NOT EXISTS idx_dim_diagnosis_category ON analytics.dim_diagnosis(category);

-- ============================================================================
-- FACT TABLES
-- ============================================================================


-- Language Usage Fact Table
CREATE TABLE IF NOT EXISTS analytics.fact_language_usage (
    fact_language_sk SERIAL PRIMARY KEY,
    report_id UUID NOT NULL UNIQUE,
    procedure_id UUID NOT NULL,
    language_code VARCHAR(10),
    language_name VARCHAR(100),
    audio_language_code VARCHAR(10),
    audio_language_name VARCHAR(100),
    has_audio BOOLEAN DEFAULT false,
    word_count INT DEFAULT 0,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fact_language_procedure ON analytics.fact_language_usage(procedure_id);
CREATE INDEX IF NOT EXISTS idx_fact_language_code ON analytics.fact_language_usage(language_code);
CREATE INDEX IF NOT EXISTS idx_fact_language_audio ON analytics.fact_language_usage(has_audio);

-- ============================================================================
-- AUDIT TABLE (ETL Tracking)
-- ============================================================================

CREATE TABLE IF NOT EXISTS analytics.etl_audit_log (
    audit_id SERIAL PRIMARY KEY,
    pipeline_name VARCHAR(100),
    pipeline_mode VARCHAR(20),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration_seconds NUMERIC,
    rows_loaded INT,
    rows_failed INT,
    status VARCHAR(20),
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_audit_pipeline_name ON analytics.etl_audit_log(pipeline_name);
CREATE INDEX IF NOT EXISTS idx_audit_start_time ON analytics.etl_audit_log(start_time);
CREATE INDEX IF NOT EXISTS idx_audit_status ON analytics.etl_audit_log(status);

-- ============================================================================
-- CONFIRMATION
-- ============================================================================

SELECT 'Analytics schema created successfully in efiche_clinical_database!' as status;

-- Verify tables exist
SELECT COUNT(*) as table_count FROM information_schema.tables 
WHERE table_schema = 'analytics' 
AND table_type = 'BASE TABLE';