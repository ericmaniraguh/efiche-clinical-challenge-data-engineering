"""
What this does for each row:

1. Facility: Checks if the hospital exists in master.facility_master. If not, inserts it.
2. Modality: Checks and inserts new imaging modality in master.modality_master.
3. Projection: Checks and inserts projection in master.projection_master.
4. Patient: Checks and inserts patient in operational.patients. 
5. Encounter: Creates a new encounter for the patient at the facility.
6. Procedure: Creates a procedure linked to the encounter, modality, and projection.    
7. Image: Creates a radiological image record linked to the procedure.
Each block uses UUIDs to uniquely identify entities.    
"""
import csv
import uuid
import random
import os
from datetime import datetime

# Determine the project root directory (assuming script is in scripts/)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)

# Input/Output files
INPUT_CSV = os.path.join(PROJECT_ROOT, 'data', 'padchest_synthetic_data.csv')
OUTPUT_SQL = os.path.join(PROJECT_ROOT, 'data', 'insert_operational_data.sql')
LIMIT = 20  # Number of rows to process

def escape_sql(val):
    if val is None or val == '':
        return 'NULL'
    return "'" + str(val).replace("'", "''") + "'"

def generate_sql():
    print(f"Reading from: {INPUT_CSV}")
    print(f"Writing to: {OUTPUT_SQL}")
    
    if not os.path.exists(INPUT_CSV):
        print(f"Error: Input file not found at {INPUT_CSV}")
        return

    with open(INPUT_CSV, 'r', encoding='utf-8') as f_in, \
         open(OUTPUT_SQL, 'w', encoding='utf-8') as f_out:
        
        reader = csv.DictReader(f_in)
        
        f_out.write("-- SQL Script to populate Operational Tables\n")
        f_out.write("-- Generated for pgAdmin execution\n")
        f_out.write("-- Handles Master Data and Foreign Keys automatically\n\n")
        
        count = 0
        for row in reader:
            if count >= LIMIT:
                break
            
            # Extract data
            pid = row.get('PatientID', '').strip()
            sex = row.get('PatientSex', 'M')
            dob = row.get('DateOfBirth', '1970-01-01')
            inst = row.get('InstitutionName', 'Unknown Hospital')
            mod = row.get('Modality', 'DX')
            proj = row.get('Projection', 'PA')
            img_id = row.get('ImageID', f'IMG_{uuid.uuid4()}')
            
            # Generate UUIDs for this transaction
            # Note: For Master data, we'll look up or create. For transactional, we create new if not exists.
            
            sql_block = f"""
DO $$
DECLARE
    v_facility_id UUID;
    v_modality_id UUID;
    v_projection_id UUID;
    v_patient_id UUID;
    v_encounter_id UUID;
    v_procedure_id UUID;
    v_image_id UUID;
BEGIN
    -- 1. FACILITY
    SELECT facility_id INTO v_facility_id FROM master.facility_master WHERE facility_code = {escape_sql(inst.upper().replace(' ', '_'))};
    IF v_facility_id IS NULL THEN
        v_facility_id := uuid_generate_v4();
        INSERT INTO master.facility_master (facility_id, facility_code, facility_name)
        VALUES (v_facility_id, {escape_sql(inst.upper().replace(' ', '_'))}, {escape_sql(inst)});
    END IF;

    -- 2. MODALITY
    SELECT modality_id INTO v_modality_id FROM master.modality_master WHERE modality_code = {escape_sql(mod)};
    IF v_modality_id IS NULL THEN
        v_modality_id := uuid_generate_v4();
        INSERT INTO master.modality_master (modality_id, modality_code, modality_name)
        VALUES (v_modality_id, {escape_sql(mod)}, {escape_sql(mod)});
    END IF;

    -- 3. PROJECTION
    SELECT projection_id INTO v_projection_id FROM master.projection_master WHERE projection_code = {escape_sql(proj)};
    IF v_projection_id IS NULL THEN
        v_projection_id := uuid_generate_v4();
        INSERT INTO master.projection_master (projection_id, projection_code, projection_name)
        VALUES (v_projection_id, {escape_sql(proj)}, {escape_sql(proj)});
    END IF;

    -- 4. PATIENT
    SELECT patient_id INTO v_patient_id FROM operational.patients WHERE patient_code = {escape_sql(pid)};
    IF v_patient_id IS NULL THEN
        v_patient_id := uuid_generate_v4();
        INSERT INTO operational.patients (patient_id, patient_code, sex, date_of_birth)
        VALUES (v_patient_id, {escape_sql(pid)}, {escape_sql(sex)}, {escape_sql(dob)}::date);
    END IF;

    -- 5. ENCOUNTER (Create new for this example)
    v_encounter_id := uuid_generate_v4();
    INSERT INTO operational.encounters (encounter_id, patient_id, facility_id, encounter_code, encounter_date)
    VALUES (v_encounter_id, v_patient_id, v_facility_id, {escape_sql(f'ENC_{uuid.uuid4()}')}, NOW());

    -- 6. PROCEDURE
    v_procedure_id := uuid_generate_v4();
    INSERT INTO operational.procedures (procedure_id, encounter_id, modality_id, projection_id, procedure_date)
    VALUES (v_procedure_id, v_encounter_id, v_modality_id, v_projection_id, NOW());

    -- 7. IMAGE
    v_image_id := uuid_generate_v4();
    INSERT INTO operational.radiological_images (image_id, procedure_id, image_code, image_filename, image_path)
    VALUES (v_image_id, v_procedure_id, {escape_sql(img_id)}, {escape_sql(img_id)}, {escape_sql(img_id)});

END $$;
"""
            f_out.write(sql_block + "\n")
            count += 1

    print(f"Successfully generated SQL for {count} rows to {OUTPUT_SQL}")

if __name__ == "__main__":
    generate_sql()
