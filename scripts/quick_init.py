#!/usr/bin/env python3
"""Quick database schema initialization"""
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = int(os.getenv('DB_PORT', '5433'))
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'admin')
DB_NAME = os.getenv('DB_NAME', 'efiche_clinical_database')

print(f"Connecting to {DB_HOST}:{DB_PORT}/{DB_NAME}...")
try:
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER,
        password=DB_PASSWORD, database=DB_NAME, connect_timeout=10
    )
    conn.autocommit = True
    cur = conn.cursor()
    
    # Check if master schema exists
    cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'master'")
    count = cur.fetchone()[0]
    print(f"Tables in master schema: {count}")
    
    if count == 0:
        print("\nCreating master schema tables...")
        
        # Create schemas
        cur.execute("CREATE SCHEMA IF NOT EXISTS master")
        cur.execute("CREATE SCHEMA IF NOT EXISTS operational")
        cur.execute("CREATE SCHEMA IF NOT EXISTS analytics")
        cur.execute("CREATE SCHEMA IF NOT EXISTS reporting")
        cur.execute("CREATE SCHEMA IF NOT EXISTS audit")
        print("✓ Schemas created")
        
        # Create master.facility_master
        cur.execute("""
            CREATE TABLE IF NOT EXISTS master.facility_master (
                facility_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                facility_code VARCHAR(50) NOT NULL UNIQUE,
                facility_name VARCHAR(255) NOT NULL UNIQUE,
                location VARCHAR(255),
                region VARCHAR(100),
                country VARCHAR(100),
                is_active BOOLEAN DEFAULT true,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        print("✓ facility_master created")
        
        # Create master.modality_master
        cur.execute("""
            CREATE TABLE IF NOT EXISTS master.modality_master (
                modality_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                modality_code VARCHAR(50) NOT NULL UNIQUE,
                modality_name VARCHAR(255) NOT NULL,
                is_active BOOLEAN DEFAULT true,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        print("✓ modality_master created")
        
        # Create master.projection_master
        cur.execute("""
            CREATE TABLE IF NOT EXISTS master.projection_master (
                projection_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                projection_code VARCHAR(50) NOT NULL UNIQUE,
                projection_name VARCHAR(255) NOT NULL,
                is_active BOOLEAN DEFAULT true,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        print("✓ projection_master created")
        
        # Create master.anatomical_region_master
        cur.execute("""
            CREATE TABLE IF NOT EXISTS master.anatomical_region_master (
                region_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                region_code VARCHAR(50) NOT NULL UNIQUE,
                region_name VARCHAR(255) NOT NULL,
                body_system VARCHAR(100),
                is_active BOOLEAN DEFAULT true,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        print("✓ anatomical_region_master created")
        
        # Create master.diagnosis_master
        cur.execute("""
            CREATE TABLE IF NOT EXISTS master.diagnosis_master (
                diagnosis_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                diagnosis_code VARCHAR(50) NOT NULL UNIQUE,
                diagnosis_name VARCHAR(255) NOT NULL,
                category VARCHAR(100),
                is_active BOOLEAN DEFAULT true,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        print("✓ diagnosis_master created")
        
        # Create master.language_registry
        cur.execute("""
            CREATE TABLE IF NOT EXISTS master.language_registry (
                language_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                language_code VARCHAR(10) NOT NULL UNIQUE,
                language_name VARCHAR(100) NOT NULL,
                is_default BOOLEAN DEFAULT false,
                is_active BOOLEAN DEFAULT true,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.execute("""
            INSERT INTO master.language_registry (language_code, language_name, is_default)
            VALUES ('en', 'English', true)
            ON CONFLICT (language_code) DO NOTHING
        """)
        print("✓ language_registry created")
        
        # Create operational schema tables
        cur.execute("""
            CREATE TABLE IF NOT EXISTS operational.patients (
                patient_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                patient_code VARCHAR(50) NOT NULL UNIQUE,
                date_of_birth DATE,
                age INT,
                sex VARCHAR(10),
                geographic_location VARCHAR(255),
                first_encounter_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_encounter_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                total_encounters INT DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        print("✓ operational.patients created")
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS operational.encounters (
                encounter_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                patient_id UUID NOT NULL REFERENCES operational.patients(patient_id),
                facility_id UUID,
                encounter_code VARCHAR(100),
                encounter_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        print("✓ operational.encounters created")
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS operational.procedures (
                procedure_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                encounter_id UUID NOT NULL REFERENCES operational.encounters(encounter_id),
                modality_id UUID,
                projection_id UUID,
                region_id UUID,
                procedure_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                procedure_notes TEXT,
                technician_name VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        print("✓ operational.procedures created")
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS operational.radiological_images (
                image_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                procedure_id UUID NOT NULL REFERENCES operational.procedures(procedure_id),
                image_code VARCHAR(255),
                image_filename VARCHAR(255),
                image_path VARCHAR(255),
                studyid VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        print("✓ operational.radiological_images created")
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS operational.findings (
                finding_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                procedure_id UUID NOT NULL REFERENCES operational.procedures(procedure_id),
                findings_text TEXT,
                impression_text TEXT,
                finding_severity VARCHAR(50),
                abnormality_detected BOOLEAN DEFAULT false,
                confidence_score DECIMAL(3,2),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        print("✓ operational.findings created")
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS operational.procedure_diagnosis (
                procedure_diagnosis_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                procedure_id UUID NOT NULL REFERENCES operational.procedures(procedure_id),
                diagnosis_id UUID,
                is_primary BOOLEAN DEFAULT false,
                is_active BOOLEAN DEFAULT true,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        print("✓ operational.procedure_diagnosis created")
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS operational.clinical_reports (
                report_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                procedure_id UUID NOT NULL REFERENCES operational.procedures(procedure_id),
                language_id UUID REFERENCES master.language_registry(language_id),
                report_text TEXT,
                report_type VARCHAR(100),
                word_count INT,
                reviewed_by VARCHAR(255),
                review_date TIMESTAMP,
                has_audio BOOLEAN DEFAULT false,
                audio_language_id UUID,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        print("✓ operational.clinical_reports created")
        
        print("\n✓ All tables created successfully!")
    else:
        print(f"\n✓ Schema already initialized with {count} tables")
    
    cur.close()
    conn.close()

except Exception as e:
    print(f"✗ Error: {e}")
    import traceback
    traceback.print_exc()
