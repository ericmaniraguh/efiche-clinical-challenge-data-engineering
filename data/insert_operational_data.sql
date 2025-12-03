-- SQL Script to populate Operational Tables
-- Generated for pgAdmin execution
-- Handles Master Data and Foreign Keys automatically


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
    SELECT facility_id INTO v_facility_id FROM master.facility_master WHERE facility_code = 'TEACHING_HOSPITAL';
    IF v_facility_id IS NULL THEN
        v_facility_id := uuid_generate_v4();
        INSERT INTO master.facility_master (facility_id, facility_code, facility_name)
        VALUES (v_facility_id, 'TEACHING_HOSPITAL', 'Teaching Hospital');
    END IF;

    -- 2. MODALITY
    SELECT modality_id INTO v_modality_id FROM master.modality_master WHERE modality_code = 'RF';
    IF v_modality_id IS NULL THEN
        v_modality_id := uuid_generate_v4();
        INSERT INTO master.modality_master (modality_id, modality_code, modality_name)
        VALUES (v_modality_id, 'RF', 'RF');
    END IF;

    -- 3. PROJECTION
    SELECT projection_id INTO v_projection_id FROM master.projection_master WHERE projection_code = 'PA';
    IF v_projection_id IS NULL THEN
        v_projection_id := uuid_generate_v4();
        INSERT INTO master.projection_master (projection_id, projection_code, projection_name)
        VALUES (v_projection_id, 'PA', 'PA');
    END IF;

    -- 4. PATIENT
    SELECT patient_id INTO v_patient_id FROM operational.patients WHERE patient_code = 'PAT000000';
    IF v_patient_id IS NULL THEN
        v_patient_id := uuid_generate_v4();
        INSERT INTO operational.patients (patient_id, patient_code, sex, date_of_birth)
        VALUES (v_patient_id, 'PAT000000', 'F', '1967-03-18'::date);
    END IF;

    -- 5. ENCOUNTER (Create new for this example)
    v_encounter_id := uuid_generate_v4();
    INSERT INTO operational.encounters (encounter_id, patient_id, facility_id, encounter_code, encounter_date)
    VALUES (v_encounter_id, v_patient_id, v_facility_id, 'ENC_d6f6edd9-e4d0-45dc-8d65-977f20709b86', NOW());

    -- 6. PROCEDURE
    v_procedure_id := uuid_generate_v4();
    INSERT INTO operational.procedures (procedure_id, encounter_id, modality_id, projection_id, procedure_date)
    VALUES (v_procedure_id, v_encounter_id, v_modality_id, v_projection_id, NOW());

    -- 7. IMAGE
    v_image_id := uuid_generate_v4();
    INSERT INTO operational.radiological_images (image_id, procedure_id, image_code, image_filename, image_path)
    VALUES (v_image_id, v_procedure_id, '43/106470496346822224221332607323372799_62-314-629.png', '43/106470496346822224221332607323372799_62-314-629.png', '43/106470496346822224221332607323372799_62-314-629.png');

END $$;


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
    SELECT facility_id INTO v_facility_id FROM master.facility_master WHERE facility_code = 'CENTRAL_CLINIC';
    IF v_facility_id IS NULL THEN
        v_facility_id := uuid_generate_v4();
        INSERT INTO master.facility_master (facility_id, facility_code, facility_name)
        VALUES (v_facility_id, 'CENTRAL_CLINIC', 'Central Clinic');
    END IF;

    -- 2. MODALITY
    SELECT modality_id INTO v_modality_id FROM master.modality_master WHERE modality_code = 'RF';
    IF v_modality_id IS NULL THEN
        v_modality_id := uuid_generate_v4();
        INSERT INTO master.modality_master (modality_id, modality_code, modality_name)
        VALUES (v_modality_id, 'RF', 'RF');
    END IF;

    -- 3. PROJECTION
    SELECT projection_id INTO v_projection_id FROM master.projection_master WHERE projection_code = 'LATERAL';
    IF v_projection_id IS NULL THEN
        v_projection_id := uuid_generate_v4();
        INSERT INTO master.projection_master (projection_id, projection_code, projection_name)
        VALUES (v_projection_id, 'LATERAL', 'LATERAL');
    END IF;

    -- 4. PATIENT
    SELECT patient_id INTO v_patient_id FROM operational.patients WHERE patient_code = 'PAT000000';
    IF v_patient_id IS NULL THEN
        v_patient_id := uuid_generate_v4();
        INSERT INTO operational.patients (patient_id, patient_code, sex, date_of_birth)
        VALUES (v_patient_id, 'PAT000000', 'M', '1967-03-18'::date);
    END IF;

    -- 5. ENCOUNTER (Create new for this example)
    v_encounter_id := uuid_generate_v4();
    INSERT INTO operational.encounters (encounter_id, patient_id, facility_id, encounter_code, encounter_date)
    VALUES (v_encounter_id, v_patient_id, v_facility_id, 'ENC_37f0af5a-81f0-44c4-b309-cc9e66bfbc8a', NOW());

    -- 6. PROCEDURE
    v_procedure_id := uuid_generate_v4();
    INSERT INTO operational.procedures (procedure_id, encounter_id, modality_id, projection_id, procedure_date)
    VALUES (v_procedure_id, v_encounter_id, v_modality_id, v_projection_id, NOW());

    -- 7. IMAGE
    v_image_id := uuid_generate_v4();
    INSERT INTO operational.radiological_images (image_id, procedure_id, image_code, image_filename, image_path)
    VALUES (v_image_id, v_procedure_id, '15/253840101736527583838030505770844813_15-200-396.png', '15/253840101736527583838030505770844813_15-200-396.png', '15/253840101736527583838030505770844813_15-200-396.png');

END $$;


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
    SELECT facility_id INTO v_facility_id FROM master.facility_master WHERE facility_code = 'CENTRAL_CLINIC';
    IF v_facility_id IS NULL THEN
        v_facility_id := uuid_generate_v4();
        INSERT INTO master.facility_master (facility_id, facility_code, facility_name)
        VALUES (v_facility_id, 'CENTRAL_CLINIC', 'Central Clinic');
    END IF;

    -- 2. MODALITY
    SELECT modality_id INTO v_modality_id FROM master.modality_master WHERE modality_code = 'CR';
    IF v_modality_id IS NULL THEN
        v_modality_id := uuid_generate_v4();
        INSERT INTO master.modality_master (modality_id, modality_code, modality_name)
        VALUES (v_modality_id, 'CR', 'CR');
    END IF;

    -- 3. PROJECTION
    SELECT projection_id INTO v_projection_id FROM master.projection_master WHERE projection_code = 'OBLIQUE';
    IF v_projection_id IS NULL THEN
        v_projection_id := uuid_generate_v4();
        INSERT INTO master.projection_master (projection_id, projection_code, projection_name)
        VALUES (v_projection_id, 'OBLIQUE', 'OBLIQUE');
    END IF;

    -- 4. PATIENT
    SELECT patient_id INTO v_patient_id FROM operational.patients WHERE patient_code = 'PAT000001';
    IF v_patient_id IS NULL THEN
        v_patient_id := uuid_generate_v4();
        INSERT INTO operational.patients (patient_id, patient_code, sex, date_of_birth)
        VALUES (v_patient_id, 'PAT000001', 'F', '2005-05-24'::date);
    END IF;

    -- 5. ENCOUNTER (Create new for this example)
    v_encounter_id := uuid_generate_v4();
    INSERT INTO operational.encounters (encounter_id, patient_id, facility_id, encounter_code, encounter_date)
    VALUES (v_encounter_id, v_patient_id, v_facility_id, 'ENC_8d286d4c-edac-49c9-b0ad-6934b2199a98', NOW());

    -- 6. PROCEDURE
    v_procedure_id := uuid_generate_v4();
    INSERT INTO operational.procedures (procedure_id, encounter_id, modality_id, projection_id, procedure_date)
    VALUES (v_procedure_id, v_encounter_id, v_modality_id, v_projection_id, NOW());

    -- 7. IMAGE
    v_image_id := uuid_generate_v4();
    INSERT INTO operational.radiological_images (image_id, procedure_id, image_code, image_filename, image_path)
    VALUES (v_image_id, v_procedure_id, '25/469476063961678539724487281339841483_06-886-896.png', '25/469476063961678539724487281339841483_06-886-896.png', '25/469476063961678539724487281339841483_06-886-896.png');

END $$;


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
    SELECT facility_id INTO v_facility_id FROM master.facility_master WHERE facility_code = 'GENERAL_HOSPITAL';
    IF v_facility_id IS NULL THEN
        v_facility_id := uuid_generate_v4();
        INSERT INTO master.facility_master (facility_id, facility_code, facility_name)
        VALUES (v_facility_id, 'GENERAL_HOSPITAL', 'General Hospital');
    END IF;

    -- 2. MODALITY
    SELECT modality_id INTO v_modality_id FROM master.modality_master WHERE modality_code = 'DX';
    IF v_modality_id IS NULL THEN
        v_modality_id := uuid_generate_v4();
        INSERT INTO master.modality_master (modality_id, modality_code, modality_name)
        VALUES (v_modality_id, 'DX', 'DX');
    END IF;

    -- 3. PROJECTION
    SELECT projection_id INTO v_projection_id FROM master.projection_master WHERE projection_code = 'OBLIQUE';
    IF v_projection_id IS NULL THEN
        v_projection_id := uuid_generate_v4();
        INSERT INTO master.projection_master (projection_id, projection_code, projection_name)
        VALUES (v_projection_id, 'OBLIQUE', 'OBLIQUE');
    END IF;

    -- 4. PATIENT
    SELECT patient_id INTO v_patient_id FROM operational.patients WHERE patient_code = 'PAT000001';
    IF v_patient_id IS NULL THEN
        v_patient_id := uuid_generate_v4();
        INSERT INTO operational.patients (patient_id, patient_code, sex, date_of_birth)
        VALUES (v_patient_id, 'PAT000001', 'M', '2005-05-24'::date);
    END IF;

    -- 5. ENCOUNTER (Create new for this example)
    v_encounter_id := uuid_generate_v4();
    INSERT INTO operational.encounters (encounter_id, patient_id, facility_id, encounter_code, encounter_date)
    VALUES (v_encounter_id, v_patient_id, v_facility_id, 'ENC_5530b362-f95c-4cbb-ac5f-dc38b842b3eb', NOW());

    -- 6. PROCEDURE
    v_procedure_id := uuid_generate_v4();
    INSERT INTO operational.procedures (procedure_id, encounter_id, modality_id, projection_id, procedure_date)
    VALUES (v_procedure_id, v_encounter_id, v_modality_id, v_projection_id, NOW());

    -- 7. IMAGE
    v_image_id := uuid_generate_v4();
    INSERT INTO operational.radiological_images (image_id, procedure_id, image_code, image_filename, image_path)
    VALUES (v_image_id, v_procedure_id, '41/357803330457201708826230074378919662_54-201-617.png', '41/357803330457201708826230074378919662_54-201-617.png', '41/357803330457201708826230074378919662_54-201-617.png');

END $$;


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
    SELECT facility_id INTO v_facility_id FROM master.facility_master WHERE facility_code = 'TEACHING_HOSPITAL';
    IF v_facility_id IS NULL THEN
        v_facility_id := uuid_generate_v4();
        INSERT INTO master.facility_master (facility_id, facility_code, facility_name)
        VALUES (v_facility_id, 'TEACHING_HOSPITAL', 'Teaching Hospital');
    END IF;

    -- 2. MODALITY
    SELECT modality_id INTO v_modality_id FROM master.modality_master WHERE modality_code = 'DX';
    IF v_modality_id IS NULL THEN
        v_modality_id := uuid_generate_v4();
        INSERT INTO master.modality_master (modality_id, modality_code, modality_name)
        VALUES (v_modality_id, 'DX', 'DX');
    END IF;

    -- 3. PROJECTION
    SELECT projection_id INTO v_projection_id FROM master.projection_master WHERE projection_code = 'OBLIQUE';
    IF v_projection_id IS NULL THEN
        v_projection_id := uuid_generate_v4();
        INSERT INTO master.projection_master (projection_id, projection_code, projection_name)
        VALUES (v_projection_id, 'OBLIQUE', 'OBLIQUE');
    END IF;

    -- 4. PATIENT
    SELECT patient_id INTO v_patient_id FROM operational.patients WHERE patient_code = 'PAT000002';
    IF v_patient_id IS NULL THEN
        v_patient_id := uuid_generate_v4();
        INSERT INTO operational.patients (patient_id, patient_code, sex, date_of_birth)
        VALUES (v_patient_id, 'PAT000002', 'M', '1992-05-22'::date);
    END IF;

    -- 5. ENCOUNTER (Create new for this example)
    v_encounter_id := uuid_generate_v4();
    INSERT INTO operational.encounters (encounter_id, patient_id, facility_id, encounter_code, encounter_date)
    VALUES (v_encounter_id, v_patient_id, v_facility_id, 'ENC_0864b31f-d3c9-4b23-b1d7-38eb394d2176', NOW());

    -- 6. PROCEDURE
    v_procedure_id := uuid_generate_v4();
    INSERT INTO operational.procedures (procedure_id, encounter_id, modality_id, projection_id, procedure_date)
    VALUES (v_procedure_id, v_encounter_id, v_modality_id, v_projection_id, NOW());

    -- 7. IMAGE
    v_image_id := uuid_generate_v4();
    INSERT INTO operational.radiological_images (image_id, procedure_id, image_code, image_filename, image_path)
    VALUES (v_image_id, v_procedure_id, '26/154401212166498203742135985914400464_31-313-816.png', '26/154401212166498203742135985914400464_31-313-816.png', '26/154401212166498203742135985914400464_31-313-816.png');

END $$;


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
    SELECT facility_id INTO v_facility_id FROM master.facility_master WHERE facility_code = 'CENTRAL_CLINIC';
    IF v_facility_id IS NULL THEN
        v_facility_id := uuid_generate_v4();
        INSERT INTO master.facility_master (facility_id, facility_code, facility_name)
        VALUES (v_facility_id, 'CENTRAL_CLINIC', 'Central Clinic');
    END IF;

    -- 2. MODALITY
    SELECT modality_id INTO v_modality_id FROM master.modality_master WHERE modality_code = 'CR';
    IF v_modality_id IS NULL THEN
        v_modality_id := uuid_generate_v4();
        INSERT INTO master.modality_master (modality_id, modality_code, modality_name)
        VALUES (v_modality_id, 'CR', 'CR');
    END IF;

    -- 3. PROJECTION
    SELECT projection_id INTO v_projection_id FROM master.projection_master WHERE projection_code = 'AP';
    IF v_projection_id IS NULL THEN
        v_projection_id := uuid_generate_v4();
        INSERT INTO master.projection_master (projection_id, projection_code, projection_name)
        VALUES (v_projection_id, 'AP', 'AP');
    END IF;

    -- 4. PATIENT
    SELECT patient_id INTO v_patient_id FROM operational.patients WHERE patient_code = 'PAT000002';
    IF v_patient_id IS NULL THEN
        v_patient_id := uuid_generate_v4();
        INSERT INTO operational.patients (patient_id, patient_code, sex, date_of_birth)
        VALUES (v_patient_id, 'PAT000002', 'F', '1992-05-22'::date);
    END IF;

    -- 5. ENCOUNTER (Create new for this example)
    v_encounter_id := uuid_generate_v4();
    INSERT INTO operational.encounters (encounter_id, patient_id, facility_id, encounter_code, encounter_date)
    VALUES (v_encounter_id, v_patient_id, v_facility_id, 'ENC_d116129f-51b8-46c7-a933-e97dbaf31891', NOW());

    -- 6. PROCEDURE
    v_procedure_id := uuid_generate_v4();
    INSERT INTO operational.procedures (procedure_id, encounter_id, modality_id, projection_id, procedure_date)
    VALUES (v_procedure_id, v_encounter_id, v_modality_id, v_projection_id, NOW());

    -- 7. IMAGE
    v_image_id := uuid_generate_v4();
    INSERT INTO operational.radiological_images (image_id, procedure_id, image_code, image_filename, image_path)
    VALUES (v_image_id, v_procedure_id, '29/704138747200545343337332689568825602_63-150-839.png', '29/704138747200545343337332689568825602_63-150-839.png', '29/704138747200545343337332689568825602_63-150-839.png');

END $$;


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
    SELECT facility_id INTO v_facility_id FROM master.facility_master WHERE facility_code = 'CENTRAL_CLINIC';
    IF v_facility_id IS NULL THEN
        v_facility_id := uuid_generate_v4();
        INSERT INTO master.facility_master (facility_id, facility_code, facility_name)
        VALUES (v_facility_id, 'CENTRAL_CLINIC', 'Central Clinic');
    END IF;

    -- 2. MODALITY
    SELECT modality_id INTO v_modality_id FROM master.modality_master WHERE modality_code = 'DR';
    IF v_modality_id IS NULL THEN
        v_modality_id := uuid_generate_v4();
        INSERT INTO master.modality_master (modality_id, modality_code, modality_name)
        VALUES (v_modality_id, 'DR', 'DR');
    END IF;

    -- 3. PROJECTION
    SELECT projection_id INTO v_projection_id FROM master.projection_master WHERE projection_code = 'PA';
    IF v_projection_id IS NULL THEN
        v_projection_id := uuid_generate_v4();
        INSERT INTO master.projection_master (projection_id, projection_code, projection_name)
        VALUES (v_projection_id, 'PA', 'PA');
    END IF;

    -- 4. PATIENT
    SELECT patient_id INTO v_patient_id FROM operational.patients WHERE patient_code = 'PAT000003';
    IF v_patient_id IS NULL THEN
        v_patient_id := uuid_generate_v4();
        INSERT INTO operational.patients (patient_id, patient_code, sex, date_of_birth)
        VALUES (v_patient_id, 'PAT000003', 'M', '1946-08-01'::date);
    END IF;

    -- 5. ENCOUNTER (Create new for this example)
    v_encounter_id := uuid_generate_v4();
    INSERT INTO operational.encounters (encounter_id, patient_id, facility_id, encounter_code, encounter_date)
    VALUES (v_encounter_id, v_patient_id, v_facility_id, 'ENC_2692c8a1-9545-4416-8192-9a29ec48ddd1', NOW());

    -- 6. PROCEDURE
    v_procedure_id := uuid_generate_v4();
    INSERT INTO operational.procedures (procedure_id, encounter_id, modality_id, projection_id, procedure_date)
    VALUES (v_procedure_id, v_encounter_id, v_modality_id, v_projection_id, NOW());

    -- 7. IMAGE
    v_image_id := uuid_generate_v4();
    INSERT INTO operational.radiological_images (image_id, procedure_id, image_code, image_filename, image_path)
    VALUES (v_image_id, v_procedure_id, '2/774827645479944893954744505782221473_96-210-432.png', '2/774827645479944893954744505782221473_96-210-432.png', '2/774827645479944893954744505782221473_96-210-432.png');

END $$;


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
    SELECT facility_id INTO v_facility_id FROM master.facility_master WHERE facility_code = 'COUNTY_MEDICAL';
    IF v_facility_id IS NULL THEN
        v_facility_id := uuid_generate_v4();
        INSERT INTO master.facility_master (facility_id, facility_code, facility_name)
        VALUES (v_facility_id, 'COUNTY_MEDICAL', 'County Medical');
    END IF;

    -- 2. MODALITY
    SELECT modality_id INTO v_modality_id FROM master.modality_master WHERE modality_code = 'RF';
    IF v_modality_id IS NULL THEN
        v_modality_id := uuid_generate_v4();
        INSERT INTO master.modality_master (modality_id, modality_code, modality_name)
        VALUES (v_modality_id, 'RF', 'RF');
    END IF;

    -- 3. PROJECTION
    SELECT projection_id INTO v_projection_id FROM master.projection_master WHERE projection_code = 'AP';
    IF v_projection_id IS NULL THEN
        v_projection_id := uuid_generate_v4();
        INSERT INTO master.projection_master (projection_id, projection_code, projection_name)
        VALUES (v_projection_id, 'AP', 'AP');
    END IF;

    -- 4. PATIENT
    SELECT patient_id INTO v_patient_id FROM operational.patients WHERE patient_code = 'PAT000003';
    IF v_patient_id IS NULL THEN
        v_patient_id := uuid_generate_v4();
        INSERT INTO operational.patients (patient_id, patient_code, sex, date_of_birth)
        VALUES (v_patient_id, 'PAT000003', 'F', '1946-08-01'::date);
    END IF;

    -- 5. ENCOUNTER (Create new for this example)
    v_encounter_id := uuid_generate_v4();
    INSERT INTO operational.encounters (encounter_id, patient_id, facility_id, encounter_code, encounter_date)
    VALUES (v_encounter_id, v_patient_id, v_facility_id, 'ENC_f1ef3864-c8ff-47b5-a871-ea903384e4f4', NOW());

    -- 6. PROCEDURE
    v_procedure_id := uuid_generate_v4();
    INSERT INTO operational.procedures (procedure_id, encounter_id, modality_id, projection_id, procedure_date)
    VALUES (v_procedure_id, v_encounter_id, v_modality_id, v_projection_id, NOW());

    -- 7. IMAGE
    v_image_id := uuid_generate_v4();
    INSERT INTO operational.radiological_images (image_id, procedure_id, image_code, image_filename, image_path)
    VALUES (v_image_id, v_procedure_id, '40/297020986439535580311087823801628695_20-464-383.png', '40/297020986439535580311087823801628695_20-464-383.png', '40/297020986439535580311087823801628695_20-464-383.png');

END $$;


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
    SELECT facility_id INTO v_facility_id FROM master.facility_master WHERE facility_code = 'TEACHING_HOSPITAL';
    IF v_facility_id IS NULL THEN
        v_facility_id := uuid_generate_v4();
        INSERT INTO master.facility_master (facility_id, facility_code, facility_name)
        VALUES (v_facility_id, 'TEACHING_HOSPITAL', 'Teaching Hospital');
    END IF;

    -- 2. MODALITY
    SELECT modality_id INTO v_modality_id FROM master.modality_master WHERE modality_code = 'RF';
    IF v_modality_id IS NULL THEN
        v_modality_id := uuid_generate_v4();
        INSERT INTO master.modality_master (modality_id, modality_code, modality_name)
        VALUES (v_modality_id, 'RF', 'RF');
    END IF;

    -- 3. PROJECTION
    SELECT projection_id INTO v_projection_id FROM master.projection_master WHERE projection_code = 'OBLIQUE';
    IF v_projection_id IS NULL THEN
        v_projection_id := uuid_generate_v4();
        INSERT INTO master.projection_master (projection_id, projection_code, projection_name)
        VALUES (v_projection_id, 'OBLIQUE', 'OBLIQUE');
    END IF;

    -- 4. PATIENT
    SELECT patient_id INTO v_patient_id FROM operational.patients WHERE patient_code = 'PAT000004';
    IF v_patient_id IS NULL THEN
        v_patient_id := uuid_generate_v4();
        INSERT INTO operational.patients (patient_id, patient_code, sex, date_of_birth)
        VALUES (v_patient_id, 'PAT000004', 'M', '1950-03-03'::date);
    END IF;

    -- 5. ENCOUNTER (Create new for this example)
    v_encounter_id := uuid_generate_v4();
    INSERT INTO operational.encounters (encounter_id, patient_id, facility_id, encounter_code, encounter_date)
    VALUES (v_encounter_id, v_patient_id, v_facility_id, 'ENC_79c7f02f-b672-42c3-996b-7cdb33241656', NOW());

    -- 6. PROCEDURE
    v_procedure_id := uuid_generate_v4();
    INSERT INTO operational.procedures (procedure_id, encounter_id, modality_id, projection_id, procedure_date)
    VALUES (v_procedure_id, v_encounter_id, v_modality_id, v_projection_id, NOW());

    -- 7. IMAGE
    v_image_id := uuid_generate_v4();
    INSERT INTO operational.radiological_images (image_id, procedure_id, image_code, image_filename, image_path)
    VALUES (v_image_id, v_procedure_id, '37/698528857999281615169063850473693788_73-189-875.png', '37/698528857999281615169063850473693788_73-189-875.png', '37/698528857999281615169063850473693788_73-189-875.png');

END $$;


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
    SELECT facility_id INTO v_facility_id FROM master.facility_master WHERE facility_code = 'CENTRAL_CLINIC';
    IF v_facility_id IS NULL THEN
        v_facility_id := uuid_generate_v4();
        INSERT INTO master.facility_master (facility_id, facility_code, facility_name)
        VALUES (v_facility_id, 'CENTRAL_CLINIC', 'Central Clinic');
    END IF;

    -- 2. MODALITY
    SELECT modality_id INTO v_modality_id FROM master.modality_master WHERE modality_code = 'DR';
    IF v_modality_id IS NULL THEN
        v_modality_id := uuid_generate_v4();
        INSERT INTO master.modality_master (modality_id, modality_code, modality_name)
        VALUES (v_modality_id, 'DR', 'DR');
    END IF;

    -- 3. PROJECTION
    SELECT projection_id INTO v_projection_id FROM master.projection_master WHERE projection_code = 'AP';
    IF v_projection_id IS NULL THEN
        v_projection_id := uuid_generate_v4();
        INSERT INTO master.projection_master (projection_id, projection_code, projection_name)
        VALUES (v_projection_id, 'AP', 'AP');
    END IF;

    -- 4. PATIENT
    SELECT patient_id INTO v_patient_id FROM operational.patients WHERE patient_code = 'PAT000004';
    IF v_patient_id IS NULL THEN
        v_patient_id := uuid_generate_v4();
        INSERT INTO operational.patients (patient_id, patient_code, sex, date_of_birth)
        VALUES (v_patient_id, 'PAT000004', 'F', '1950-03-03'::date);
    END IF;

    -- 5. ENCOUNTER (Create new for this example)
    v_encounter_id := uuid_generate_v4();
    INSERT INTO operational.encounters (encounter_id, patient_id, facility_id, encounter_code, encounter_date)
    VALUES (v_encounter_id, v_patient_id, v_facility_id, 'ENC_0606ad14-c2c4-4c85-948c-38361cf5981a', NOW());

    -- 6. PROCEDURE
    v_procedure_id := uuid_generate_v4();
    INSERT INTO operational.procedures (procedure_id, encounter_id, modality_id, projection_id, procedure_date)
    VALUES (v_procedure_id, v_encounter_id, v_modality_id, v_projection_id, NOW());

    -- 7. IMAGE
    v_image_id := uuid_generate_v4();
    INSERT INTO operational.radiological_images (image_id, procedure_id, image_code, image_filename, image_path)
    VALUES (v_image_id, v_procedure_id, '13/329932168081995999988135035929943429_50-486-146.png', '13/329932168081995999988135035929943429_50-486-146.png', '13/329932168081995999988135035929943429_50-486-146.png');

END $$;


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
    SELECT facility_id INTO v_facility_id FROM master.facility_master WHERE facility_code = 'TEACHING_HOSPITAL';
    IF v_facility_id IS NULL THEN
        v_facility_id := uuid_generate_v4();
        INSERT INTO master.facility_master (facility_id, facility_code, facility_name)
        VALUES (v_facility_id, 'TEACHING_HOSPITAL', 'Teaching Hospital');
    END IF;

    -- 2. MODALITY
    SELECT modality_id INTO v_modality_id FROM master.modality_master WHERE modality_code = 'RF';
    IF v_modality_id IS NULL THEN
        v_modality_id := uuid_generate_v4();
        INSERT INTO master.modality_master (modality_id, modality_code, modality_name)
        VALUES (v_modality_id, 'RF', 'RF');
    END IF;

    -- 3. PROJECTION
    SELECT projection_id INTO v_projection_id FROM master.projection_master WHERE projection_code = 'OBLIQUE';
    IF v_projection_id IS NULL THEN
        v_projection_id := uuid_generate_v4();
        INSERT INTO master.projection_master (projection_id, projection_code, projection_name)
        VALUES (v_projection_id, 'OBLIQUE', 'OBLIQUE');
    END IF;

    -- 4. PATIENT
    SELECT patient_id INTO v_patient_id FROM operational.patients WHERE patient_code = 'PAT000005';
    IF v_patient_id IS NULL THEN
        v_patient_id := uuid_generate_v4();
        INSERT INTO operational.patients (patient_id, patient_code, sex, date_of_birth)
        VALUES (v_patient_id, 'PAT000005', 'M', '1943-03-18'::date);
    END IF;

    -- 5. ENCOUNTER (Create new for this example)
    v_encounter_id := uuid_generate_v4();
    INSERT INTO operational.encounters (encounter_id, patient_id, facility_id, encounter_code, encounter_date)
    VALUES (v_encounter_id, v_patient_id, v_facility_id, 'ENC_44ae9766-b89e-4885-ab5f-9ff297530f31', NOW());

    -- 6. PROCEDURE
    v_procedure_id := uuid_generate_v4();
    INSERT INTO operational.procedures (procedure_id, encounter_id, modality_id, projection_id, procedure_date)
    VALUES (v_procedure_id, v_encounter_id, v_modality_id, v_projection_id, NOW());

    -- 7. IMAGE
    v_image_id := uuid_generate_v4();
    INSERT INTO operational.radiological_images (image_id, procedure_id, image_code, image_filename, image_path)
    VALUES (v_image_id, v_procedure_id, '6/346516921277093696273358805882753093_85-350-023.png', '6/346516921277093696273358805882753093_85-350-023.png', '6/346516921277093696273358805882753093_85-350-023.png');

END $$;


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
    SELECT facility_id INTO v_facility_id FROM master.facility_master WHERE facility_code = 'GENERAL_HOSPITAL';
    IF v_facility_id IS NULL THEN
        v_facility_id := uuid_generate_v4();
        INSERT INTO master.facility_master (facility_id, facility_code, facility_name)
        VALUES (v_facility_id, 'GENERAL_HOSPITAL', 'General Hospital');
    END IF;

    -- 2. MODALITY
    SELECT modality_id INTO v_modality_id FROM master.modality_master WHERE modality_code = 'RF';
    IF v_modality_id IS NULL THEN
        v_modality_id := uuid_generate_v4();
        INSERT INTO master.modality_master (modality_id, modality_code, modality_name)
        VALUES (v_modality_id, 'RF', 'RF');
    END IF;

    -- 3. PROJECTION
    SELECT projection_id INTO v_projection_id FROM master.projection_master WHERE projection_code = 'PA';
    IF v_projection_id IS NULL THEN
        v_projection_id := uuid_generate_v4();
        INSERT INTO master.projection_master (projection_id, projection_code, projection_name)
        VALUES (v_projection_id, 'PA', 'PA');
    END IF;

    -- 4. PATIENT
    SELECT patient_id INTO v_patient_id FROM operational.patients WHERE patient_code = 'PAT000005';
    IF v_patient_id IS NULL THEN
        v_patient_id := uuid_generate_v4();
        INSERT INTO operational.patients (patient_id, patient_code, sex, date_of_birth)
        VALUES (v_patient_id, 'PAT000005', 'M', '1943-03-18'::date);
    END IF;

    -- 5. ENCOUNTER (Create new for this example)
    v_encounter_id := uuid_generate_v4();
    INSERT INTO operational.encounters (encounter_id, patient_id, facility_id, encounter_code, encounter_date)
    VALUES (v_encounter_id, v_patient_id, v_facility_id, 'ENC_aa33bafb-599f-4b10-874b-55fbd9f98d9b', NOW());

    -- 6. PROCEDURE
    v_procedure_id := uuid_generate_v4();
    INSERT INTO operational.procedures (procedure_id, encounter_id, modality_id, projection_id, procedure_date)
    VALUES (v_procedure_id, v_encounter_id, v_modality_id, v_projection_id, NOW());

    -- 7. IMAGE
    v_image_id := uuid_generate_v4();
    INSERT INTO operational.radiological_images (image_id, procedure_id, image_code, image_filename, image_path)
    VALUES (v_image_id, v_procedure_id, '10/601975460085269228365543812308712997_91-797-402.png', '10/601975460085269228365543812308712997_91-797-402.png', '10/601975460085269228365543812308712997_91-797-402.png');

END $$;


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
    SELECT facility_id INTO v_facility_id FROM master.facility_master WHERE facility_code = 'TEACHING_HOSPITAL';
    IF v_facility_id IS NULL THEN
        v_facility_id := uuid_generate_v4();
        INSERT INTO master.facility_master (facility_id, facility_code, facility_name)
        VALUES (v_facility_id, 'TEACHING_HOSPITAL', 'Teaching Hospital');
    END IF;

    -- 2. MODALITY
    SELECT modality_id INTO v_modality_id FROM master.modality_master WHERE modality_code = 'DX';
    IF v_modality_id IS NULL THEN
        v_modality_id := uuid_generate_v4();
        INSERT INTO master.modality_master (modality_id, modality_code, modality_name)
        VALUES (v_modality_id, 'DX', 'DX');
    END IF;

    -- 3. PROJECTION
    SELECT projection_id INTO v_projection_id FROM master.projection_master WHERE projection_code = 'LATERAL';
    IF v_projection_id IS NULL THEN
        v_projection_id := uuid_generate_v4();
        INSERT INTO master.projection_master (projection_id, projection_code, projection_name)
        VALUES (v_projection_id, 'LATERAL', 'LATERAL');
    END IF;

    -- 4. PATIENT
    SELECT patient_id INTO v_patient_id FROM operational.patients WHERE patient_code = 'PAT000006';
    IF v_patient_id IS NULL THEN
        v_patient_id := uuid_generate_v4();
        INSERT INTO operational.patients (patient_id, patient_code, sex, date_of_birth)
        VALUES (v_patient_id, 'PAT000006', 'M', '1989-11-27'::date);
    END IF;

    -- 5. ENCOUNTER (Create new for this example)
    v_encounter_id := uuid_generate_v4();
    INSERT INTO operational.encounters (encounter_id, patient_id, facility_id, encounter_code, encounter_date)
    VALUES (v_encounter_id, v_patient_id, v_facility_id, 'ENC_8ba1db69-c1b3-44f2-a9f9-ab064a6eb8ad', NOW());

    -- 6. PROCEDURE
    v_procedure_id := uuid_generate_v4();
    INSERT INTO operational.procedures (procedure_id, encounter_id, modality_id, projection_id, procedure_date)
    VALUES (v_procedure_id, v_encounter_id, v_modality_id, v_projection_id, NOW());

    -- 7. IMAGE
    v_image_id := uuid_generate_v4();
    INSERT INTO operational.radiological_images (image_id, procedure_id, image_code, image_filename, image_path)
    VALUES (v_image_id, v_procedure_id, '26/804701614853999844222892001374523590_22-311-977.png', '26/804701614853999844222892001374523590_22-311-977.png', '26/804701614853999844222892001374523590_22-311-977.png');

END $$;


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
    SELECT facility_id INTO v_facility_id FROM master.facility_master WHERE facility_code = 'GENERAL_HOSPITAL';
    IF v_facility_id IS NULL THEN
        v_facility_id := uuid_generate_v4();
        INSERT INTO master.facility_master (facility_id, facility_code, facility_name)
        VALUES (v_facility_id, 'GENERAL_HOSPITAL', 'General Hospital');
    END IF;

    -- 2. MODALITY
    SELECT modality_id INTO v_modality_id FROM master.modality_master WHERE modality_code = 'RF';
    IF v_modality_id IS NULL THEN
        v_modality_id := uuid_generate_v4();
        INSERT INTO master.modality_master (modality_id, modality_code, modality_name)
        VALUES (v_modality_id, 'RF', 'RF');
    END IF;

    -- 3. PROJECTION
    SELECT projection_id INTO v_projection_id FROM master.projection_master WHERE projection_code = 'LATERAL';
    IF v_projection_id IS NULL THEN
        v_projection_id := uuid_generate_v4();
        INSERT INTO master.projection_master (projection_id, projection_code, projection_name)
        VALUES (v_projection_id, 'LATERAL', 'LATERAL');
    END IF;

    -- 4. PATIENT
    SELECT patient_id INTO v_patient_id FROM operational.patients WHERE patient_code = 'PAT000006';
    IF v_patient_id IS NULL THEN
        v_patient_id := uuid_generate_v4();
        INSERT INTO operational.patients (patient_id, patient_code, sex, date_of_birth)
        VALUES (v_patient_id, 'PAT000006', 'F', '1989-11-27'::date);
    END IF;

    -- 5. ENCOUNTER (Create new for this example)
    v_encounter_id := uuid_generate_v4();
    INSERT INTO operational.encounters (encounter_id, patient_id, facility_id, encounter_code, encounter_date)
    VALUES (v_encounter_id, v_patient_id, v_facility_id, 'ENC_30f1c473-5210-47da-b8fc-bee5b1797261', NOW());

    -- 6. PROCEDURE
    v_procedure_id := uuid_generate_v4();
    INSERT INTO operational.procedures (procedure_id, encounter_id, modality_id, projection_id, procedure_date)
    VALUES (v_procedure_id, v_encounter_id, v_modality_id, v_projection_id, NOW());

    -- 7. IMAGE
    v_image_id := uuid_generate_v4();
    INSERT INTO operational.radiological_images (image_id, procedure_id, image_code, image_filename, image_path)
    VALUES (v_image_id, v_procedure_id, '23/356546357072590353434573059457983515_78-546-871.png', '23/356546357072590353434573059457983515_78-546-871.png', '23/356546357072590353434573059457983515_78-546-871.png');

END $$;


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
    SELECT facility_id INTO v_facility_id FROM master.facility_master WHERE facility_code = 'TEACHING_HOSPITAL';
    IF v_facility_id IS NULL THEN
        v_facility_id := uuid_generate_v4();
        INSERT INTO master.facility_master (facility_id, facility_code, facility_name)
        VALUES (v_facility_id, 'TEACHING_HOSPITAL', 'Teaching Hospital');
    END IF;

    -- 2. MODALITY
    SELECT modality_id INTO v_modality_id FROM master.modality_master WHERE modality_code = 'CR';
    IF v_modality_id IS NULL THEN
        v_modality_id := uuid_generate_v4();
        INSERT INTO master.modality_master (modality_id, modality_code, modality_name)
        VALUES (v_modality_id, 'CR', 'CR');
    END IF;

    -- 3. PROJECTION
    SELECT projection_id INTO v_projection_id FROM master.projection_master WHERE projection_code = 'LATERAL';
    IF v_projection_id IS NULL THEN
        v_projection_id := uuid_generate_v4();
        INSERT INTO master.projection_master (projection_id, projection_code, projection_name)
        VALUES (v_projection_id, 'LATERAL', 'LATERAL');
    END IF;

    -- 4. PATIENT
    SELECT patient_id INTO v_patient_id FROM operational.patients WHERE patient_code = 'PAT000007';
    IF v_patient_id IS NULL THEN
        v_patient_id := uuid_generate_v4();
        INSERT INTO operational.patients (patient_id, patient_code, sex, date_of_birth)
        VALUES (v_patient_id, 'PAT000007', 'F', '1986-12-05'::date);
    END IF;

    -- 5. ENCOUNTER (Create new for this example)
    v_encounter_id := uuid_generate_v4();
    INSERT INTO operational.encounters (encounter_id, patient_id, facility_id, encounter_code, encounter_date)
    VALUES (v_encounter_id, v_patient_id, v_facility_id, 'ENC_7cd06ab8-f7e3-4146-b708-de4e908b45cd', NOW());

    -- 6. PROCEDURE
    v_procedure_id := uuid_generate_v4();
    INSERT INTO operational.procedures (procedure_id, encounter_id, modality_id, projection_id, procedure_date)
    VALUES (v_procedure_id, v_encounter_id, v_modality_id, v_projection_id, NOW());

    -- 7. IMAGE
    v_image_id := uuid_generate_v4();
    INSERT INTO operational.radiological_images (image_id, procedure_id, image_code, image_filename, image_path)
    VALUES (v_image_id, v_procedure_id, '31/403201783327822869361972607281837408_42-188-233.png', '31/403201783327822869361972607281837408_42-188-233.png', '31/403201783327822869361972607281837408_42-188-233.png');

END $$;


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
    SELECT facility_id INTO v_facility_id FROM master.facility_master WHERE facility_code = 'CENTRAL_CLINIC';
    IF v_facility_id IS NULL THEN
        v_facility_id := uuid_generate_v4();
        INSERT INTO master.facility_master (facility_id, facility_code, facility_name)
        VALUES (v_facility_id, 'CENTRAL_CLINIC', 'Central Clinic');
    END IF;

    -- 2. MODALITY
    SELECT modality_id INTO v_modality_id FROM master.modality_master WHERE modality_code = 'CR';
    IF v_modality_id IS NULL THEN
        v_modality_id := uuid_generate_v4();
        INSERT INTO master.modality_master (modality_id, modality_code, modality_name)
        VALUES (v_modality_id, 'CR', 'CR');
    END IF;

    -- 3. PROJECTION
    SELECT projection_id INTO v_projection_id FROM master.projection_master WHERE projection_code = 'LATERAL';
    IF v_projection_id IS NULL THEN
        v_projection_id := uuid_generate_v4();
        INSERT INTO master.projection_master (projection_id, projection_code, projection_name)
        VALUES (v_projection_id, 'LATERAL', 'LATERAL');
    END IF;

    -- 4. PATIENT
    SELECT patient_id INTO v_patient_id FROM operational.patients WHERE patient_code = 'PAT000007';
    IF v_patient_id IS NULL THEN
        v_patient_id := uuid_generate_v4();
        INSERT INTO operational.patients (patient_id, patient_code, sex, date_of_birth)
        VALUES (v_patient_id, 'PAT000007', 'M', '1986-12-05'::date);
    END IF;

    -- 5. ENCOUNTER (Create new for this example)
    v_encounter_id := uuid_generate_v4();
    INSERT INTO operational.encounters (encounter_id, patient_id, facility_id, encounter_code, encounter_date)
    VALUES (v_encounter_id, v_patient_id, v_facility_id, 'ENC_e553ee79-3061-4148-b375-f223632d791b', NOW());

    -- 6. PROCEDURE
    v_procedure_id := uuid_generate_v4();
    INSERT INTO operational.procedures (procedure_id, encounter_id, modality_id, projection_id, procedure_date)
    VALUES (v_procedure_id, v_encounter_id, v_modality_id, v_projection_id, NOW());

    -- 7. IMAGE
    v_image_id := uuid_generate_v4();
    INSERT INTO operational.radiological_images (image_id, procedure_id, image_code, image_filename, image_path)
    VALUES (v_image_id, v_procedure_id, '8/769179808171492119057957271419877254_95-743-082.png', '8/769179808171492119057957271419877254_95-743-082.png', '8/769179808171492119057957271419877254_95-743-082.png');

END $$;


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
    SELECT facility_id INTO v_facility_id FROM master.facility_master WHERE facility_code = 'COUNTY_MEDICAL';
    IF v_facility_id IS NULL THEN
        v_facility_id := uuid_generate_v4();
        INSERT INTO master.facility_master (facility_id, facility_code, facility_name)
        VALUES (v_facility_id, 'COUNTY_MEDICAL', 'County Medical');
    END IF;

    -- 2. MODALITY
    SELECT modality_id INTO v_modality_id FROM master.modality_master WHERE modality_code = 'DX';
    IF v_modality_id IS NULL THEN
        v_modality_id := uuid_generate_v4();
        INSERT INTO master.modality_master (modality_id, modality_code, modality_name)
        VALUES (v_modality_id, 'DX', 'DX');
    END IF;

    -- 3. PROJECTION
    SELECT projection_id INTO v_projection_id FROM master.projection_master WHERE projection_code = 'AP';
    IF v_projection_id IS NULL THEN
        v_projection_id := uuid_generate_v4();
        INSERT INTO master.projection_master (projection_id, projection_code, projection_name)
        VALUES (v_projection_id, 'AP', 'AP');
    END IF;

    -- 4. PATIENT
    SELECT patient_id INTO v_patient_id FROM operational.patients WHERE patient_code = 'PAT000007';
    IF v_patient_id IS NULL THEN
        v_patient_id := uuid_generate_v4();
        INSERT INTO operational.patients (patient_id, patient_code, sex, date_of_birth)
        VALUES (v_patient_id, 'PAT000007', 'F', '1986-12-05'::date);
    END IF;

    -- 5. ENCOUNTER (Create new for this example)
    v_encounter_id := uuid_generate_v4();
    INSERT INTO operational.encounters (encounter_id, patient_id, facility_id, encounter_code, encounter_date)
    VALUES (v_encounter_id, v_patient_id, v_facility_id, 'ENC_3ca995b4-b700-4b2f-8391-7b9806734f33', NOW());

    -- 6. PROCEDURE
    v_procedure_id := uuid_generate_v4();
    INSERT INTO operational.procedures (procedure_id, encounter_id, modality_id, projection_id, procedure_date)
    VALUES (v_procedure_id, v_encounter_id, v_modality_id, v_projection_id, NOW());

    -- 7. IMAGE
    v_image_id := uuid_generate_v4();
    INSERT INTO operational.radiological_images (image_id, procedure_id, image_code, image_filename, image_path)
    VALUES (v_image_id, v_procedure_id, '7/893603074623598709607730658082366998_29-021-342.png', '7/893603074623598709607730658082366998_29-021-342.png', '7/893603074623598709607730658082366998_29-021-342.png');

END $$;


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
    SELECT facility_id INTO v_facility_id FROM master.facility_master WHERE facility_code = 'GENERAL_HOSPITAL';
    IF v_facility_id IS NULL THEN
        v_facility_id := uuid_generate_v4();
        INSERT INTO master.facility_master (facility_id, facility_code, facility_name)
        VALUES (v_facility_id, 'GENERAL_HOSPITAL', 'General Hospital');
    END IF;

    -- 2. MODALITY
    SELECT modality_id INTO v_modality_id FROM master.modality_master WHERE modality_code = 'CR';
    IF v_modality_id IS NULL THEN
        v_modality_id := uuid_generate_v4();
        INSERT INTO master.modality_master (modality_id, modality_code, modality_name)
        VALUES (v_modality_id, 'CR', 'CR');
    END IF;

    -- 3. PROJECTION
    SELECT projection_id INTO v_projection_id FROM master.projection_master WHERE projection_code = 'LATERAL';
    IF v_projection_id IS NULL THEN
        v_projection_id := uuid_generate_v4();
        INSERT INTO master.projection_master (projection_id, projection_code, projection_name)
        VALUES (v_projection_id, 'LATERAL', 'LATERAL');
    END IF;

    -- 4. PATIENT
    SELECT patient_id INTO v_patient_id FROM operational.patients WHERE patient_code = 'PAT000008';
    IF v_patient_id IS NULL THEN
        v_patient_id := uuid_generate_v4();
        INSERT INTO operational.patients (patient_id, patient_code, sex, date_of_birth)
        VALUES (v_patient_id, 'PAT000008', 'F', '1938-12-27'::date);
    END IF;

    -- 5. ENCOUNTER (Create new for this example)
    v_encounter_id := uuid_generate_v4();
    INSERT INTO operational.encounters (encounter_id, patient_id, facility_id, encounter_code, encounter_date)
    VALUES (v_encounter_id, v_patient_id, v_facility_id, 'ENC_5cbc1af8-4f47-4591-987f-2583388e435f', NOW());

    -- 6. PROCEDURE
    v_procedure_id := uuid_generate_v4();
    INSERT INTO operational.procedures (procedure_id, encounter_id, modality_id, projection_id, procedure_date)
    VALUES (v_procedure_id, v_encounter_id, v_modality_id, v_projection_id, NOW());

    -- 7. IMAGE
    v_image_id := uuid_generate_v4();
    INSERT INTO operational.radiological_images (image_id, procedure_id, image_code, image_filename, image_path)
    VALUES (v_image_id, v_procedure_id, '41/093121192805835691402037135884161841_09-874-582.png', '41/093121192805835691402037135884161841_09-874-582.png', '41/093121192805835691402037135884161841_09-874-582.png');

END $$;


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
    SELECT facility_id INTO v_facility_id FROM master.facility_master WHERE facility_code = 'TEACHING_HOSPITAL';
    IF v_facility_id IS NULL THEN
        v_facility_id := uuid_generate_v4();
        INSERT INTO master.facility_master (facility_id, facility_code, facility_name)
        VALUES (v_facility_id, 'TEACHING_HOSPITAL', 'Teaching Hospital');
    END IF;

    -- 2. MODALITY
    SELECT modality_id INTO v_modality_id FROM master.modality_master WHERE modality_code = 'DX';
    IF v_modality_id IS NULL THEN
        v_modality_id := uuid_generate_v4();
        INSERT INTO master.modality_master (modality_id, modality_code, modality_name)
        VALUES (v_modality_id, 'DX', 'DX');
    END IF;

    -- 3. PROJECTION
    SELECT projection_id INTO v_projection_id FROM master.projection_master WHERE projection_code = 'OBLIQUE';
    IF v_projection_id IS NULL THEN
        v_projection_id := uuid_generate_v4();
        INSERT INTO master.projection_master (projection_id, projection_code, projection_name)
        VALUES (v_projection_id, 'OBLIQUE', 'OBLIQUE');
    END IF;

    -- 4. PATIENT
    SELECT patient_id INTO v_patient_id FROM operational.patients WHERE patient_code = 'PAT000009';
    IF v_patient_id IS NULL THEN
        v_patient_id := uuid_generate_v4();
        INSERT INTO operational.patients (patient_id, patient_code, sex, date_of_birth)
        VALUES (v_patient_id, 'PAT000009', 'M', '1985-12-24'::date);
    END IF;

    -- 5. ENCOUNTER (Create new for this example)
    v_encounter_id := uuid_generate_v4();
    INSERT INTO operational.encounters (encounter_id, patient_id, facility_id, encounter_code, encounter_date)
    VALUES (v_encounter_id, v_patient_id, v_facility_id, 'ENC_e9e7c4d0-f319-4040-b85b-dbcdf35fafe1', NOW());

    -- 6. PROCEDURE
    v_procedure_id := uuid_generate_v4();
    INSERT INTO operational.procedures (procedure_id, encounter_id, modality_id, projection_id, procedure_date)
    VALUES (v_procedure_id, v_encounter_id, v_modality_id, v_projection_id, NOW());

    -- 7. IMAGE
    v_image_id := uuid_generate_v4();
    INSERT INTO operational.radiological_images (image_id, procedure_id, image_code, image_filename, image_path)
    VALUES (v_image_id, v_procedure_id, '47/931410543242649246289427115722698309_72-996-993.png', '47/931410543242649246289427115722698309_72-996-993.png', '47/931410543242649246289427115722698309_72-996-993.png');

END $$;


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
    SELECT facility_id INTO v_facility_id FROM master.facility_master WHERE facility_code = 'GENERAL_HOSPITAL';
    IF v_facility_id IS NULL THEN
        v_facility_id := uuid_generate_v4();
        INSERT INTO master.facility_master (facility_id, facility_code, facility_name)
        VALUES (v_facility_id, 'GENERAL_HOSPITAL', 'General Hospital');
    END IF;

    -- 2. MODALITY
    SELECT modality_id INTO v_modality_id FROM master.modality_master WHERE modality_code = 'DX';
    IF v_modality_id IS NULL THEN
        v_modality_id := uuid_generate_v4();
        INSERT INTO master.modality_master (modality_id, modality_code, modality_name)
        VALUES (v_modality_id, 'DX', 'DX');
    END IF;

    -- 3. PROJECTION
    SELECT projection_id INTO v_projection_id FROM master.projection_master WHERE projection_code = 'LATERAL';
    IF v_projection_id IS NULL THEN
        v_projection_id := uuid_generate_v4();
        INSERT INTO master.projection_master (projection_id, projection_code, projection_name)
        VALUES (v_projection_id, 'LATERAL', 'LATERAL');
    END IF;

    -- 4. PATIENT
    SELECT patient_id INTO v_patient_id FROM operational.patients WHERE patient_code = 'PAT000009';
    IF v_patient_id IS NULL THEN
        v_patient_id := uuid_generate_v4();
        INSERT INTO operational.patients (patient_id, patient_code, sex, date_of_birth)
        VALUES (v_patient_id, 'PAT000009', 'F', '1985-12-24'::date);
    END IF;

    -- 5. ENCOUNTER (Create new for this example)
    v_encounter_id := uuid_generate_v4();
    INSERT INTO operational.encounters (encounter_id, patient_id, facility_id, encounter_code, encounter_date)
    VALUES (v_encounter_id, v_patient_id, v_facility_id, 'ENC_20dc623e-66fb-4adc-b8f0-9fbd47805a28', NOW());

    -- 6. PROCEDURE
    v_procedure_id := uuid_generate_v4();
    INSERT INTO operational.procedures (procedure_id, encounter_id, modality_id, projection_id, procedure_date)
    VALUES (v_procedure_id, v_encounter_id, v_modality_id, v_projection_id, NOW());

    -- 7. IMAGE
    v_image_id := uuid_generate_v4();
    INSERT INTO operational.radiological_images (image_id, procedure_id, image_code, image_filename, image_path)
    VALUES (v_image_id, v_procedure_id, '31/048528782948627306718893386163463626_85-785-157.png', '31/048528782948627306718893386163463626_85-785-157.png', '31/048528782948627306718893386163463626_85-785-157.png');

END $$;

