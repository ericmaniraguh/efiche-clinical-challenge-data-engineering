-- ============================================================================
-- eFiche Clinical Imaging Data Warehouse - v4.2 CORRECTED
-- ENGLISH AS DEFAULT LANGUAGE
-- PostgreSQL 9.6+ Compatible
-- FIXES APPLIED:
--   1. SET search_path AFTER schema creation
--   2. Language ID default via TRIGGER (not function call)
--   3. Data validation via CHECK constraints
--   4. Audit triggers for compliance logging
-- ============================================================================

-- Enable extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- ============================================================================
-- CREATE SCHEMAS (Must be FIRST)
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS master;      -- Reference data
CREATE SCHEMA IF NOT EXISTS operational; -- Core transaction data
CREATE SCHEMA IF NOT EXISTS reporting;   -- Views & aggregates
CREATE SCHEMA IF NOT EXISTS audit;       -- Compliance & tracking
CREATE SCHEMA IF NOT EXISTS staging;     -- Temporary data

-- Set default search path (AFTER schemas are created)
ALTER DATABASE efiche_clinical_database SET search_path TO operational, master, reporting, audit, public;


-- ============================================================================
-- MASTER SCHEMA - Reference/Lookup Tables
-- ============================================================================

-- ============================================================================
-- 1. LANGUAGE_REGISTRY (Multi-language support)
-- English is first (will be default)
-- ============================================================================

CREATE TABLE master.language_registry (
    language_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    language_code VARCHAR(10) NOT NULL UNIQUE,
    language_name VARCHAR(100) NOT NULL,
    is_default BOOLEAN DEFAULT false,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO master.language_registry (language_code, language_name, is_default) VALUES
    ('en', 'English', true),      -- DEFAULT
    ('fr', 'French', false),
    ('sw', 'Swahili', false),
    ('rw', 'Kinyarwanda', false),
    ('es', 'Spanish', false),
    ('de', 'German', false)
ON CONFLICT (language_code) DO NOTHING;

CREATE INDEX idx_language_code ON master.language_registry(language_code);
CREATE INDEX idx_language_default ON master.language_registry(is_default);


-- ============================================================================
-- Helper Function: Get Default (English) Language ID
-- ============================================================================

CREATE OR REPLACE FUNCTION master.get_default_language_id()
RETURNS UUID AS $$
DECLARE
    v_language_id UUID;
BEGIN
    SELECT language_id INTO v_language_id 
    FROM master.language_registry 
    WHERE language_code = 'en' 
    LIMIT 1;
    
    IF v_language_id IS NULL THEN
        RAISE EXCEPTION 'English language not found in language_registry';
    END IF;
    
    RETURN v_language_id;
END;
$$ LANGUAGE plpgsql STABLE;


-- ============================================================================
-- 2. FACILITY_MASTER
-- ============================================================================

CREATE TABLE master.facility_master (
    facility_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    facility_code VARCHAR(50) NOT NULL UNIQUE,
    facility_name VARCHAR(255) NOT NULL UNIQUE,
    facility_type VARCHAR(100),
    location VARCHAR(255),
    region VARCHAR(100),
    country VARCHAR(100),
    contact_phone VARCHAR(20),
    contact_email VARCHAR(255),
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_facility_name ON master.facility_master(facility_name);
CREATE INDEX idx_facility_location ON master.facility_master(location);


-- ============================================================================
-- 3. MODALITY_MASTER
-- ============================================================================

CREATE TABLE master.modality_master (
    modality_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    modality_code VARCHAR(20) NOT NULL UNIQUE,
    modality_name VARCHAR(100) NOT NULL,
    description TEXT,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_modality_code ON master.modality_master(modality_code);


-- ============================================================================
-- 4. PROJECTION_MASTER
-- ============================================================================

CREATE TABLE master.projection_master (
    projection_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    projection_code VARCHAR(20) NOT NULL UNIQUE,
    projection_name VARCHAR(100) NOT NULL,
    description TEXT,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_projection_code ON master.projection_master(projection_code);


-- ============================================================================
-- 5. ANATOMICAL_REGION_MASTER
-- ============================================================================

CREATE TABLE master.anatomical_region_master (
    region_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    region_code VARCHAR(50) NOT NULL UNIQUE,
    region_name VARCHAR(255) NOT NULL,
    body_system VARCHAR(100),
    snomed_code VARCHAR(20),
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_region_code ON master.anatomical_region_master(region_code);


-- ============================================================================
-- 6. DIAGNOSIS_MASTER
-- ============================================================================

CREATE TABLE master.diagnosis_master (
    diagnosis_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    diagnosis_code VARCHAR(50) NOT NULL UNIQUE,
    diagnosis_name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    description TEXT,
    icd10_code VARCHAR(10),
    snomed_code VARCHAR(20),
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_diagnosis_code ON master.diagnosis_master(diagnosis_code);
CREATE INDEX idx_diagnosis_category ON master.diagnosis_master(category);


-- ============================================================================
-- OPERATIONAL SCHEMA - Core Transaction Tables
-- ============================================================================

-- ============================================================================
-- 7. PATIENTS
-- ============================================================================

CREATE TABLE operational.patients (
    patient_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    patient_code VARCHAR(50) NOT NULL UNIQUE,
    date_of_birth DATE,
    age INT CHECK (age >= 0 AND age <= 150),
    sex VARCHAR(10),
    height INT CHECK (height >= 0 AND height <= 300),
    weight INT CHECK (weight >= 0 AND weight <= 500),
    geographic_location VARCHAR(255),
    first_encounter_date TIMESTAMP,
    last_encounter_date TIMESTAMP,
    total_encounters INT DEFAULT 0 CHECK (total_encounters >= 0),
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_patients_code ON operational.patients(patient_code);
CREATE INDEX idx_patients_age ON operational.patients(age);
CREATE INDEX idx_patients_location ON operational.patients(geographic_location);


-- ============================================================================
-- 8. ENCOUNTERS
-- ============================================================================

CREATE TABLE operational.encounters (
    encounter_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    patient_id UUID NOT NULL REFERENCES operational.patients(patient_id) ON DELETE CASCADE,
    facility_id UUID NOT NULL REFERENCES master.facility_master(facility_id) ON DELETE RESTRICT,
    encounter_code VARCHAR(50) NOT NULL UNIQUE,
    encounter_date TIMESTAMP NOT NULL,
    encounter_type VARCHAR(100) DEFAULT 'Imaging Study',
    referring_physician VARCHAR(255),
    notes TEXT,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_encounters_patient ON operational.encounters(patient_id);
CREATE INDEX idx_encounters_facility ON operational.encounters(facility_id);
CREATE INDEX idx_encounters_code ON operational.encounters(encounter_code);
CREATE INDEX idx_encounters_date ON operational.encounters(encounter_date);


-- ============================================================================
-- 9. PROCEDURES
-- ============================================================================

CREATE TABLE operational.procedures (
    procedure_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    encounter_id UUID NOT NULL REFERENCES operational.encounters(encounter_id) ON DELETE CASCADE,
    modality_id UUID NOT NULL REFERENCES master.modality_master(modality_id),
    projection_id UUID REFERENCES master.projection_master(projection_id),
    region_id UUID REFERENCES master.anatomical_region_master(region_id),
    procedure_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    procedure_notes TEXT,
    image_count INT DEFAULT 1 CHECK (image_count >= 1),
    technician_name VARCHAR(255),
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_procedures_encounter ON operational.procedures(encounter_id);
CREATE INDEX idx_procedures_modality ON operational.procedures(modality_id);
CREATE INDEX idx_procedures_date ON operational.procedures(procedure_date);


-- ============================================================================
-- 10. RADIOLOGICAL_IMAGES
-- ============================================================================

CREATE TABLE operational.radiological_images (
    image_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    procedure_id UUID NOT NULL REFERENCES operational.procedures(procedure_id) ON DELETE CASCADE,
    image_code VARCHAR(100) NOT NULL UNIQUE,
    image_filename VARCHAR(255) NOT NULL,
    image_path TEXT NOT NULL,
    StudyID TEXT,
    file_size INT CHECK (file_size >= 0),
    file_format VARCHAR(50),
    image_sequence INT,
    checksum VARCHAR(64),
    image_embedding BYTEA,
    embedding_dimension INT CHECK (embedding_dimension >= 0),
    embedding_model VARCHAR(100),
    embedding_extracted_at TIMESTAMP,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_radiological_images_procedure ON operational.radiological_images(procedure_id);
CREATE INDEX idx_radiological_images_code ON operational.radiological_images(image_code);


-- ============================================================================
-- 11. CLINICAL_REPORTS (DEFAULT LANGUAGE = ENGLISH via TRIGGER)
-- ============================================================================

CREATE TABLE operational.clinical_reports (
    report_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    procedure_id UUID NOT NULL REFERENCES operational.procedures(procedure_id) ON DELETE CASCADE,
    language_id UUID NOT NULL REFERENCES master.language_registry(language_id),
    report_text TEXT NOT NULL,
    report_type VARCHAR(100),
    word_count INT CHECK (word_count >= 0),
    text_embedding BYTEA,
    embedding_dimension INT CHECK (embedding_dimension >= 0),
    embedding_model VARCHAR(100),
    embedding_extracted_at TIMESTAMP,
    has_audio BOOLEAN DEFAULT false,
    audio_path TEXT,
    audio_duration_seconds INT CHECK (audio_duration_seconds >= 0),
    audio_language_id UUID REFERENCES master.language_registry(language_id),
    audio_transcription TEXT,
    reviewed_by VARCHAR(255),
    review_date TIMESTAMP,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_clinical_reports_procedure ON operational.clinical_reports(procedure_id);
CREATE INDEX idx_clinical_reports_language ON operational.clinical_reports(language_id);
CREATE INDEX idx_clinical_reports_audio_language ON operational.clinical_reports(audio_language_id);


-- ============================================================================
-- TRIGGER: Set English as default language for clinical_reports
-- ============================================================================

CREATE OR REPLACE FUNCTION operational.set_default_language()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.language_id IS NULL THEN
        NEW.language_id := master.get_default_language_id();
    END IF;
    IF NEW.audio_language_id IS NULL AND NEW.has_audio = true THEN
        NEW.audio_language_id := master.get_default_language_id();
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_clinical_reports_default_language
BEFORE INSERT ON operational.clinical_reports
FOR EACH ROW
EXECUTE FUNCTION operational.set_default_language();


-- ============================================================================
-- 12. FINDINGS
-- ============================================================================

CREATE TABLE operational.findings (
    finding_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    procedure_id UUID NOT NULL REFERENCES operational.procedures(procedure_id) ON DELETE CASCADE,
    findings_text TEXT NOT NULL,
    impression_text TEXT,
    finding_severity VARCHAR(50) CHECK (finding_severity IN ('Normal', 'Mild', 'Moderate', 'Severe', 'Critical')),
    abnormality_detected BOOLEAN DEFAULT false,
    confidence_score FLOAT CHECK (confidence_score >= 0 AND confidence_score <= 1),
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_findings_procedure ON operational.findings(procedure_id);
CREATE INDEX idx_findings_abnormality ON operational.findings(abnormality_detected);


-- ============================================================================
-- 13. PROCEDURE_DIAGNOSIS (Bridge Table)
-- ============================================================================

CREATE TABLE operational.procedure_diagnosis (
    pd_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    procedure_id UUID NOT NULL REFERENCES operational.procedures(procedure_id) ON DELETE CASCADE,
    diagnosis_id UUID NOT NULL REFERENCES master.diagnosis_master(diagnosis_id) ON DELETE RESTRICT,
    diagnosis_sequence INT DEFAULT 1 CHECK (diagnosis_sequence >= 1),
    is_primary BOOLEAN DEFAULT false,
    confidence_score FLOAT CHECK (confidence_score >= 0 AND confidence_score <= 1),
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_pd_procedure ON operational.procedure_diagnosis(procedure_id);
CREATE INDEX idx_pd_diagnosis ON operational.procedure_diagnosis(diagnosis_id);
CREATE UNIQUE INDEX idx_pd_unique ON operational.procedure_diagnosis(procedure_id, diagnosis_id);


-- ============================================================================
-- AUDIT SCHEMA - Compliance & Tracking
-- ============================================================================

-- ============================================================================
-- 14. AUDIT_LOG
-- ============================================================================

CREATE TABLE audit.audit_log (
    log_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    schema_name VARCHAR(100),
    table_name VARCHAR(100) NOT NULL,
    record_id UUID NOT NULL,
    action VARCHAR(20) NOT NULL CHECK (action IN ('INSERT', 'UPDATE', 'DELETE')),
    old_values TEXT,
    new_values TEXT,
    changed_by VARCHAR(255),
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_audit_table ON audit.audit_log(table_name);
CREATE INDEX idx_audit_record ON audit.audit_log(record_id);
CREATE INDEX idx_audit_timestamp ON audit.audit_log(changed_at);


-- ============================================================================
-- 15. DATA_QUALITY_LOG
-- ============================================================================

CREATE TABLE audit.data_quality_log (
    qc_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    schema_name VARCHAR(100),
    table_name VARCHAR(100) NOT NULL,
    record_id UUID,
    issue_type VARCHAR(100),
    issue_description TEXT,
    severity VARCHAR(50) CHECK (severity IN ('INFO', 'WARNING', 'ERROR', 'CRITICAL')),
    is_resolved BOOLEAN DEFAULT false,
    is_synthetic_data BOOLEAN DEFAULT false,
    embedding_status VARCHAR(50),
    resolved_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_qc_table ON audit.data_quality_log(table_name);
CREATE INDEX idx_qc_synthetic ON audit.data_quality_log(is_synthetic_data);


-- ============================================================================
-- AUDIT TRIGGERS (Populate audit_log automatically)
-- ============================================================================

CREATE OR REPLACE FUNCTION audit.log_changes()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO audit.audit_log (schema_name, table_name, record_id, action, new_values, changed_by)
        VALUES (TG_TABLE_SCHEMA, TG_TABLE_NAME, NEW.patient_id, TG_OP, row_to_json(NEW), CURRENT_USER);
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO audit.audit_log (schema_name, table_name, record_id, action, old_values, new_values, changed_by)
        VALUES (TG_TABLE_SCHEMA, TG_TABLE_NAME, NEW.patient_id, TG_OP, row_to_json(OLD), row_to_json(NEW), CURRENT_USER);
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO audit.audit_log (schema_name, table_name, record_id, action, old_values, changed_by)
        VALUES (TG_TABLE_SCHEMA, TG_TABLE_NAME, OLD.patient_id, TG_OP, row_to_json(OLD), CURRENT_USER);
        RETURN OLD;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- STAGING SCHEMA - Temporary Data
-- ============================================================================
-- ============================================================================
-- NEW: Table-Specific Audit Trigger Functions
-- ============================================================================

-- Trigger for PATIENTS table (uses patient_id)
CREATE OR REPLACE FUNCTION audit.log_changes_patients()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO audit.audit_log (schema_name, table_name, record_id, action, new_values, changed_by)
        VALUES (TG_TABLE_SCHEMA, TG_TABLE_NAME, NEW.patient_id, TG_OP, row_to_json(NEW), CURRENT_USER);
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO audit.audit_log (schema_name, table_name, record_id, action, old_values, new_values, changed_by)
        VALUES (TG_TABLE_SCHEMA, TG_TABLE_NAME, NEW.patient_id, TG_OP, row_to_json(OLD), row_to_json(NEW), CURRENT_USER);
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO audit.audit_log (schema_name, table_name, record_id, action, old_values, changed_by)
        VALUES (TG_TABLE_SCHEMA, TG_TABLE_NAME, OLD.patient_id, TG_OP, row_to_json(OLD), CURRENT_USER);
        RETURN OLD;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Trigger for ENCOUNTERS table (uses encounter_id)
CREATE OR REPLACE FUNCTION audit.log_changes_encounters()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO audit.audit_log (schema_name, table_name, record_id, action, new_values, changed_by)
        VALUES (TG_TABLE_SCHEMA, TG_TABLE_NAME, NEW.encounter_id, TG_OP, row_to_json(NEW), CURRENT_USER);
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO audit.audit_log (schema_name, table_name, record_id, action, old_values, new_values, changed_by)
        VALUES (TG_TABLE_SCHEMA, TG_TABLE_NAME, NEW.encounter_id, TG_OP, row_to_json(OLD), row_to_json(NEW), CURRENT_USER);
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO audit.audit_log (schema_name, table_name, record_id, action, old_values, changed_by)
        VALUES (TG_TABLE_SCHEMA, TG_TABLE_NAME, OLD.encounter_id, TG_OP, row_to_json(OLD), CURRENT_USER);
        RETURN OLD;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Trigger for CLINICAL_REPORTS table (uses report_id)
CREATE OR REPLACE FUNCTION audit.log_changes_clinical_reports()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO audit.audit_log (schema_name, table_name, record_id, action, new_values, changed_by)
        VALUES (TG_TABLE_SCHEMA, TG_TABLE_NAME, NEW.report_id, TG_OP, row_to_json(NEW), CURRENT_USER);
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO audit.audit_log (schema_name, table_name, record_id, action, old_values, new_values, changed_by)
        VALUES (TG_TABLE_SCHEMA, TG_TABLE_NAME, NEW.report_id, TG_OP, row_to_json(OLD), row_to_json(NEW), CURRENT_USER);
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO audit.audit_log (schema_name, table_name, record_id, action, old_values, changed_by)
        VALUES (TG_TABLE_SCHEMA, TG_TABLE_NAME, OLD.report_id, TG_OP, row_to_json(OLD), CURRENT_USER);
        RETURN OLD;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- Create New Table-Specific Triggers
-- ============================================================================

-- Drop existing triggers if they exist (for idempotency)
DROP TRIGGER IF EXISTS trg_audit_patients ON operational.patients;
DROP TRIGGER IF EXISTS trg_audit_encounters ON operational.encounters;
DROP TRIGGER IF EXISTS trg_audit_clinical_reports ON operational.clinical_reports;

-- Create table-specific triggers
CREATE TRIGGER trg_audit_patients AFTER INSERT OR UPDATE OR DELETE ON operational.patients
FOR EACH ROW EXECUTE FUNCTION audit.log_changes_patients();

CREATE TRIGGER trg_audit_encounters AFTER INSERT OR UPDATE OR DELETE ON operational.encounters
FOR EACH ROW EXECUTE FUNCTION audit.log_changes_encounters();

CREATE TRIGGER trg_audit_clinical_reports AFTER INSERT OR UPDATE OR DELETE ON operational.clinical_reports
FOR EACH ROW EXECUTE FUNCTION audit.log_changes_clinical_reports();

-- ============================================================================
-- CSV Import Staging Table
-- ============================================================================

CREATE TABLE staging.csv_import_staging (
    staging_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    ImageID TEXT,
    PatientID TEXT,
    StudyID TEXT,
    PatientAge INT,
    PatientSex TEXT,
    PatientHeight INT,
    PatientWeight INT,
    StudyDate TEXT,
    Projection TEXT,
    Modality TEXT,
    Location TEXT,
    InstitutionName TEXT,
    Labels TEXT,
    Report TEXT,
    Findings TEXT,
    Impression TEXT,
    CreatedAt TEXT,
    import_status VARCHAR(50) DEFAULT 'pending' CHECK (import_status IN ('pending', 'processing', 'success', 'failed')),
    import_error TEXT,
    imported_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_staging_status ON staging.csv_import_staging(import_status);


-- ============================================================================
-- REPORTING SCHEMA - Views & Aggregates
-- ============================================================================

-- ============================================================================
-- View 1: Encounter Summary (With Language Names)
-- ============================================================================

CREATE VIEW reporting.encounter_summary AS
SELECT 
    e.encounter_id,
    e.encounter_code,
    p.patient_code,
    p.age,
    p.sex,
    fm.facility_name,
    fm.location as facility_location,
    e.encounter_date,
    mm.modality_name,
    pm.projection_name,
    arm.region_name,
    cr.report_text,
    lr.language_name as report_language,
    COALESCE(lr_audio.language_name, 'English') as audio_language,
    fi.findings_text,
    fi.impression_text,
    COUNT(DISTINCT ri.image_id) as total_images
FROM operational.encounters e
JOIN operational.patients p ON e.patient_id = p.patient_id
JOIN master.facility_master fm ON e.facility_id = fm.facility_id
JOIN operational.procedures pr ON e.encounter_id = pr.encounter_id
JOIN master.modality_master mm ON pr.modality_id = mm.modality_id
LEFT JOIN master.projection_master pm ON pr.projection_id = pm.projection_id
LEFT JOIN master.anatomical_region_master arm ON pr.region_id = arm.region_id
LEFT JOIN operational.clinical_reports cr ON pr.procedure_id = cr.procedure_id
LEFT JOIN master.language_registry lr ON cr.language_id = lr.language_id
LEFT JOIN master.language_registry lr_audio ON cr.audio_language_id = lr_audio.language_id
LEFT JOIN operational.findings fi ON pr.procedure_id = fi.procedure_id
LEFT JOIN operational.radiological_images ri ON pr.procedure_id = ri.procedure_id
GROUP BY 
    e.encounter_id, e.encounter_code, p.patient_code, p.age, p.sex,
    fm.facility_name, fm.location, e.encounter_date, mm.modality_name,
    pm.projection_name, arm.region_name, cr.report_text, lr.language_name,
    lr_audio.language_name, fi.findings_text, fi.impression_text;


-- ============================================================================
-- View 2: Diagnosis by Location (Malaria Surveillance)
-- ============================================================================

CREATE VIEW reporting.diagnosis_by_location AS
SELECT 
    p.geographic_location,
    d.diagnosis_name,
    d.category,
    COUNT(DISTINCT e.patient_id) as patient_count,
    COUNT(DISTINCT e.encounter_id) as encounter_count,
    COUNT(DISTINCT pd.pd_id) as diagnosis_count,
    MAX(e.encounter_date) as last_occurrence
FROM operational.patients p
JOIN operational.encounters e ON p.patient_id = e.patient_id
JOIN operational.procedures pr ON e.encounter_id = pr.encounter_id
JOIN operational.procedure_diagnosis pd ON pr.procedure_id = pd.procedure_id
JOIN master.diagnosis_master d ON pd.diagnosis_id = d.diagnosis_id
WHERE pd.is_active = true
GROUP BY p.geographic_location, d.diagnosis_name, d.category
ORDER BY p.geographic_location, diagnosis_count DESC;


-- ============================================================================
-- View 3: Language Distribution (Multi-Language Usage)
-- ============================================================================

CREATE VIEW reporting.language_distribution AS
SELECT 
    lr.language_name,
    lr.language_code,
    COUNT(DISTINCT cr.report_id) as report_count,
    COUNT(DISTINCT cr.procedure_id) as procedure_count,
    ROUND(100.0 * COUNT(DISTINCT cr.report_id) / 
        NULLIF((SELECT COUNT(*) FROM operational.clinical_reports), 0), 2) as percentage
FROM operational.clinical_reports cr
JOIN master.language_registry lr ON cr.language_id = lr.language_id
GROUP BY lr.language_name, lr.language_code
ORDER BY report_count DESC;


-- ============================================================================
-- STORED PROCEDURES
-- ============================================================================

-- ============================================================================
-- Procedure 1: Insert Complete Encounter (With English Default)
-- ============================================================================

CREATE OR REPLACE FUNCTION operational.insert_complete_encounter(
    p_patient_id UUID,
    p_facility_id UUID,
    p_encounter_code VARCHAR,
    p_encounter_date TIMESTAMP,
    p_modality_id UUID,
    p_projection_id UUID,
    p_region_id UUID,
    p_image_code VARCHAR,
    p_image_filename VARCHAR,
    p_image_path TEXT,
    p_report_text TEXT,
    p_language_code VARCHAR DEFAULT 'en',
    p_findings_text TEXT DEFAULT NULL,
    p_impression_text TEXT DEFAULT NULL
) RETURNS UUID AS $$
DECLARE
    v_encounter_id UUID;
    v_procedure_id UUID;
    v_language_id UUID;
BEGIN
    -- Get language ID (default to 'en')
    IF p_language_code IS NULL OR p_language_code = '' THEN
        p_language_code := 'en';
    END IF;
    
    SELECT language_id INTO v_language_id 
    FROM master.language_registry 
    WHERE language_code = p_language_code
    LIMIT 1;
    
    IF v_language_id IS NULL THEN
        v_language_id := master.get_default_language_id();
    END IF;
    
    -- Create encounter
    INSERT INTO operational.encounters 
    (patient_id, facility_id, encounter_code, encounter_date)
    VALUES 
    (p_patient_id, p_facility_id, p_encounter_code, p_encounter_date)
    RETURNING encounter_id INTO v_encounter_id;
    
    -- Create procedure
    INSERT INTO operational.procedures 
    (encounter_id, modality_id, projection_id, region_id)
    VALUES 
    (v_encounter_id, p_modality_id, p_projection_id, p_region_id)
    RETURNING procedure_id INTO v_procedure_id;
    
    -- Insert image
    INSERT INTO operational.radiological_images 
    (procedure_id, image_code, image_filename, image_path)
    VALUES 
    (v_procedure_id, p_image_code, p_image_filename, p_image_path);
    
    -- Insert report (language_id defaults to English via trigger)
    IF p_report_text IS NOT NULL AND p_report_text != '' THEN
        INSERT INTO operational.clinical_reports 
        (procedure_id, language_id, report_text)
        VALUES 
        (v_procedure_id, v_language_id, p_report_text);
    END IF;
    
    -- Insert findings
    IF p_findings_text IS NOT NULL AND p_findings_text != '' THEN
        INSERT INTO operational.findings 
        (procedure_id, findings_text, impression_text)
        VALUES 
        (v_procedure_id, p_findings_text, p_impression_text);
    END IF;
    
    RETURN v_encounter_id;
END;
$$ LANGUAGE plpgsql;


-- ============================================================================
-- GRANTS & PERMISSIONS (Fixed - compatible with older PostgreSQL)
-- ============================================================================

-- Create application role (check if exists, then create)
DO $$
BEGIN
    CREATE ROLE app_role WITH LOGIN PASSWORD 'change_me_in_production';
EXCEPTION WHEN OTHERS THEN
    NULL; -- Role already exists
END
$$;

-- Grant schema usage
GRANT USAGE ON SCHEMA master TO app_role;
GRANT USAGE ON SCHEMA operational TO app_role;
GRANT USAGE ON SCHEMA reporting TO app_role;
GRANT USAGE ON SCHEMA audit TO app_role;

-- Grant table permissions
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA operational TO app_role;
GRANT SELECT ON ALL TABLES IN SCHEMA master TO app_role;
GRANT SELECT ON ALL TABLES IN SCHEMA reporting TO app_role;
GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA audit TO app_role;

-- Grant function permissions
GRANT EXECUTE ON FUNCTION master.get_default_language_id() TO app_role;
GRANT EXECUTE ON FUNCTION operational.insert_complete_encounter(UUID, UUID, VARCHAR, TIMESTAMP, UUID, UUID, UUID, VARCHAR, VARCHAR, TEXT, TEXT, VARCHAR, TEXT, TEXT) TO app_role;
GRANT EXECUTE ON FUNCTION audit.log_changes() TO app_role;
GRANT EXECUTE ON FUNCTION operational.set_default_language() TO app_role;

-- Create analytics role (read-only)
DO $$
BEGIN
    CREATE ROLE analytics_role WITH LOGIN PASSWORD 'change_me_in_production';
EXCEPTION WHEN OTHERS THEN
    NULL; -- Role already exists
END
$$;

GRANT USAGE ON SCHEMA reporting TO analytics_role;
GRANT SELECT ON ALL TABLES IN SCHEMA reporting TO analytics_role;


-- ============================================================================
-- COMMENTS (Documentation)
-- ============================================================================

COMMENT ON TABLE operational.clinical_reports IS 'Clinical reports with multi-language support. language_id defaults to English (en) via trigger.';
COMMENT ON COLUMN operational.clinical_reports.language_id IS 'Language of the report text. Defaults to English via BEFORE INSERT trigger. Foreign key to language_registry.language_id';
COMMENT ON COLUMN operational.clinical_reports.audio_language_id IS 'Language of the audio transcription. Defaults to English via trigger if has_audio = true.';
COMMENT ON FUNCTION master.get_default_language_id() IS 'Returns the UUID of the default language (English). Used by triggers as default value for language_id columns.';
COMMENT ON TABLE master.language_registry IS 'Supported languages with is_default flag. English (en) is the default language.';
COMMENT ON FUNCTION audit.log_changes() IS 'Audit trigger function. Logs INSERT, UPDATE, DELETE operations to audit.audit_log table for compliance tracking.';
COMMENT ON FUNCTION operational.set_default_language() IS 'BEFORE INSERT trigger for clinical_reports. Sets language_id to English if NULL. Sets audio_language_id to English if has_audio=true and NULL.';

-- ============================================================================
-- END OF SCHEMA v4.2 CORRECTED (ENGLISH DEFAULT + AUDIT TRIGGERS)
-- ============================================================================