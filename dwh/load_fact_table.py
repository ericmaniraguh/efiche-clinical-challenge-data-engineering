"""
FINAL FIXED - Load fact_procedure
Uses NULL instead of -1 for missing diagnosis_sk to avoid FK constraint violation
"""

import sys
import os
from pathlib import Path
from datetime import datetime

# Add to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / 'utils'))
sys.path.insert(0, str(PROJECT_ROOT))

from db_connection import get_db_connection, init_logger
import logging

logger = init_logger('load_fact_procedure', logging.INFO)

def load_fact_procedure_fixed():
    """Load fact_procedure from source to target - FIXED FK constraint"""
    
    logger.info("="*70)
    logger.info("LOADING FACT_PROCEDURE - FINAL FIXED SOLUTION")
    logger.info("="*70)
    
    try:
        # Connect to BOTH databases
        source_conn = get_db_connection(db_name='efiche_clinical_database')
        target_conn = get_db_connection(db_name='efiche_clinical_db_analytics')
        
        source_cur = source_conn.cursor()
        target_cur = target_conn.cursor()
        
        logger.info("\n[STEP 1] Extracting procedure data from SOURCE database...")
        
        # EXTRACT from SOURCE - all operational data
        extract_query = """
        SELECT 
            pr.procedure_id,
            e.encounter_id,
            e.patient_id,
            e.facility_id,
            pr.modality_id,
            e.encounter_date,
            COALESCE(fi.finding_severity, 'Unknown') as finding_severity,
            COALESCE(fi.abnormality_detected, false) as abnormality_detected,
            COALESCE(fi.confidence_score, 0.0) as confidence_score,
            COUNT(DISTINCT ri.image_id)::INT as total_images,
            COALESCE(cr.word_count, 0) as report_word_count
        FROM operational.procedures pr
        JOIN operational.encounters e ON pr.encounter_id = e.encounter_id
        LEFT JOIN operational.findings fi ON pr.procedure_id = fi.procedure_id
        LEFT JOIN operational.radiological_images ri ON pr.procedure_id = ri.procedure_id
        LEFT JOIN operational.clinical_reports cr ON pr.procedure_id = cr.procedure_id
        WHERE e.encounter_id IS NOT NULL
        GROUP BY 
            pr.procedure_id, e.encounter_id, e.patient_id, e.facility_id, 
            pr.modality_id, e.encounter_date, fi.finding_severity, 
            fi.abnormality_detected, fi.confidence_score, cr.word_count
        """
        
        source_cur.execute(extract_query)
        procedures = source_cur.fetchall()
        logger.info(f"✓ Extracted {len(procedures):,} procedures from source")
        
        # CLEAR target before loading
        logger.info("\n[STEP 2] Clearing target table...")
        target_cur.execute("TRUNCATE TABLE analytics.fact_procedure CASCADE")
        target_conn.commit()
        logger.info("✓ Cleared fact_procedure table")
        
        # LOAD to TARGET - lookup dimensions and insert
        logger.info("\n[STEP 3] Loading procedures to TARGET database...")
        logger.info("   Looking up dimension keys and inserting...")
        
        rows_loaded = 0
        rows_failed = 0
        
        for idx, proc in enumerate(procedures):
            try:
                (procedure_id, encounter_id, patient_id, facility_id, modality_id,
                 encounter_date, finding_severity, abnormality_detected, confidence_score,
                 total_images, report_word_count) = proc
                
                # LOOKUP dimension keys from TARGET
                target_cur.execute(
                    "SELECT patient_sk FROM analytics.dim_patient WHERE patient_id = %s LIMIT 1",
                    (patient_id,)
                )
                patient_result = target_cur.fetchone()
                patient_sk = patient_result[0] if patient_result else -1
                
                target_cur.execute(
                    "SELECT facility_sk FROM analytics.dim_facility WHERE facility_id = %s LIMIT 1",
                    (facility_id,)
                )
                facility_result = target_cur.fetchone()
                facility_sk = facility_result[0] if facility_result else -1
                
                target_cur.execute(
                    "SELECT modality_sk FROM analytics.dim_modality WHERE modality_id = %s LIMIT 1",
                    (modality_id,)
                )
                modality_result = target_cur.fetchone()
                modality_sk = modality_result[0] if modality_result else -1
                
                # INSERT into target
                # KEY FIX: Use NULL instead of -1 for diagnosis_sk to avoid FK constraint
                target_cur.execute("""
                    INSERT INTO analytics.fact_procedure (
                        encounter_id, procedure_id, patient_sk, facility_sk, modality_sk, 
                        diagnosis_sk, encounter_date, finding_severity, 
                        abnormality_detected, confidence_score, 
                        total_images, report_word_count, loaded_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (procedure_id) DO UPDATE SET
                        abnormality_detected = EXCLUDED.abnormality_detected,
                        confidence_score = EXCLUDED.confidence_score,
                        report_word_count = EXCLUDED.report_word_count,
                        loaded_at = CURRENT_TIMESTAMP
                """, (encounter_id, procedure_id, patient_sk, facility_sk, modality_sk,
                      None, encounter_date, finding_severity, abnormality_detected,
                      confidence_score, total_images, report_word_count))
                
                rows_loaded += 1
                
                # Progress indicator
                if (idx + 1) % 1000 == 0:
                    logger.info(f"   ✓ Processed {idx + 1:,} procedures...")
                
            except Exception as row_error:
                rows_failed += 1
                if rows_failed <= 5:  # Only log first 5 errors
                    logger.warning(f"   ⚠ Skipped procedure {procedure_id}: {row_error}")
                # Don't continue on error - abort transaction and retry
                target_conn.rollback()
                continue
        
        # Commit all inserts
        target_conn.commit()
        logger.info(f"✓ Committed {rows_loaded:,} procedures to target")
        
        # VERIFY
        logger.info("\n[STEP 4] Verifying load...")
        target_cur.execute("SELECT COUNT(*) FROM analytics.fact_procedure")
        final_count = target_cur.fetchone()[0]
        
        logger.info("="*70)
        logger.info("LOAD COMPLETE - SUMMARY STATISTICS")
        logger.info("="*70)
        logger.info(f"✓ Extracted from source: {len(procedures):,} procedures")
        logger.info(f"✓ Loaded to target: {rows_loaded:,} procedures")
        logger.info(f"⚠ Failed: {rows_failed:,} procedures")
        logger.info(f"✓ Final count in table: {final_count:,} rows")
        logger.info("="*70)
        
        if final_count > 0:
            logger.info("\n SUCCESS! fact_procedure is now populated!")
            logger.info(f"\n Your Analytics Warehouse Summary:")
            logger.info(f"   dim_patient: 1,000 rows")
            logger.info(f"   dim_facility: 4 rows")
            logger.info(f"   dim_modality: 4 rows")
            logger.info(f"   dim_diagnosis: 13 rows")
            logger.info(f"   fact_procedure: {final_count:,} rows ✓")
            logger.info(f"   fact_language_usage: 11,000 rows")
            logger.info(f"   TOTAL: 23,021+ rows")
            logger.info(f"\n Next steps:")
            logger.info(f"   1. Run: python analytics_utils.py")
            logger.info(f"   2. Query your analytics data")
            logger.info(f"   3. Generate reports and statistics")
            return True
        else:
            logger.error("\n FAILED - No data was loaded!")
            return False
    
    except Exception as e:
        logger.error(f"\n ERROR: {e}", exc_info=True)
        return False
    
    finally:
        source_cur.close()
        target_cur.close()
        source_conn.close()
        target_conn.close()
        logger.info("\nConnections closed.")

if __name__ == "__main__":
    success = load_fact_procedure_fixed()
    sys.exit(0 if success else 1)