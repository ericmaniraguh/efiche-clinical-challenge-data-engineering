"""
Diagnostic Script - Check why ETL isn't loading data
Verifies source tables exist and have data
"""

import sys
import os
from pathlib import Path

# Add to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / 'utils'))
sys.path.insert(0, str(PROJECT_ROOT))

from db_connection import get_db_connection, init_logger
import logging

logger = init_logger('diagnostics', logging.INFO)

def check_source_database():
    """Check what tables exist in source database"""
    logger.info("="*70)
    logger.info("DIAGNOSTIC - CHECKING SOURCE DATABASE")
    logger.info("="*70)
    
    try:
        conn = get_db_connection(db_name='efiche_clinical_database')
        cur = conn.cursor()
        
        # Get all schemas
        logger.info("\n[1] Checking schemas in efiche_clinical_database:")
        cur.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'public')
            ORDER BY schema_name
        """)
        
        schemas = cur.fetchall()
        for schema in schemas:
            logger.info(f"    - {schema[0]}")
        
        # Check operational schema tables
        logger.info("\n[2] Tables in operational schema:")
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'operational'
            ORDER BY table_name
        """)
        
        tables = cur.fetchall()
        if tables:
            for table in tables:
                logger.info(f"    - {table[0]}")
        else:
            logger.warning("    ⚠ No tables found in operational schema!")
        
        # Check master schema tables
        logger.info("\n[3] Tables in master schema:")
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'master'
            ORDER BY table_name
        """)
        
        tables = cur.fetchall()
        if tables:
            for table in tables:
                logger.info(f"    - {table[0]}")
        else:
            logger.warning("    ⚠ No tables found in master schema!")
        
        # Check data in operational tables
        logger.info("\n[4] Row counts in operational schema tables:")
        cur.execute("""
            SELECT table_name, 
                   (xpath('/row/cnt/text()', xml_count))[1]::text::int as cnt
            FROM (
                SELECT table_name, 
                       query_to_xml(format('SELECT count(*) as cnt FROM %I.%I', 
                                          table_schema, table_name), true, true, '') AS xml_count
                FROM information_schema.tables 
                WHERE table_schema = 'operational'
            ) t
            ORDER BY table_name
        """)
        
        try:
            counts = cur.fetchall()
            for table_name, count in counts:
                logger.info(f"    - {table_name}: {count} rows")
        except Exception as e:
            # Fallback method - check specific tables
            logger.info("    (Using fallback method)")
            
            # Try to get patients
            try:
                cur.execute("SELECT COUNT(*) FROM operational.patients")
                count = cur.fetchone()[0]
                logger.info(f"    - patients: {count} rows")
            except:
                logger.warning("    - patients: table not found")
            
            # Try to get encounters
            try:
                cur.execute("SELECT COUNT(*) FROM operational.encounters")
                count = cur.fetchone()[0]
                logger.info(f"    - encounters: {count} rows")
            except:
                logger.warning("    - encounters: table not found")
            
            # Try to get procedures
            try:
                cur.execute("SELECT COUNT(*) FROM operational.procedures")
                count = cur.fetchone()[0]
                logger.info(f"    - procedures: {count} rows")
            except:
                logger.warning("    - procedures: table not found")
        
        # Check master tables
        logger.info("\n[5] Row counts in master schema tables:")
        
        master_tables = [
            'facility_master',
            'modality_master',
            'diagnosis_master',
            'projection_master',
            'anatomical_region_master',
            'language_registry'
        ]
        
        for table in master_tables:
            try:
                cur.execute(f"SELECT COUNT(*) FROM master.{table}")
                count = cur.fetchone()[0]
                logger.info(f"    - {table}: {count} rows")
            except Exception as e:
                logger.warning(f"    - {table}: not found or error")
        
        cur.close()
        conn.close()
        
        return True
    
    except Exception as e:
        logger.error(f"Error checking source database: {e}")
        return False

def check_target_database():
    """Check what's in target database"""
    logger.info("\n" + "="*70)
    logger.info("DIAGNOSTIC - CHECKING TARGET DATABASE")
    logger.info("="*70)
    
    try:
        conn = get_db_connection(db_name='efiche_clinical_db_analytics')
        cur = conn.cursor()
        
        logger.info("\n[6] Analytics schema tables and row counts:")
        
        analytics_tables = [
            'dim_patient',
            'dim_facility',
            'dim_modality',
            'dim_diagnosis',
            'fact_procedure',
            'fact_language_usage',
            'etl_audit_log'
        ]
        
        for table in analytics_tables:
            try:
                cur.execute(f"SELECT COUNT(*) FROM analytics.{table}")
                count = cur.fetchone()[0]
                status = "✓" if count > 0 else "✗"
                logger.info(f"    {status} {table}: {count} rows")
            except Exception as e:
                logger.warning(f"    ? {table}: error - {e}")
        
        cur.close()
        conn.close()
        
        return True
    
    except Exception as e:
        logger.error(f"Error checking target database: {e}")
        return False

def compare_and_diagnose():
    """Compare source and target and diagnose issues"""
    logger.info("\n" + "="*70)
    logger.info("DIAGNOSIS")
    logger.info("="*70)
    
    try:
        source_conn = get_db_connection(db_name='efiche_clinical_database')
        target_conn = get_db_connection(db_name='efiche_clinical_db_analytics')
        
        source_cur = source_conn.cursor()
        target_cur = target_conn.cursor()
        
        # Check if operational.patients has data
        logger.info("\n[7] Source data verification:")
        
        try:
            source_cur.execute("SELECT COUNT(*) FROM operational.patients")
            patient_count = source_cur.fetchone()[0]
            logger.info(f"    operational.patients: {patient_count} rows")
            
            if patient_count > 0:
                logger.info("    ✓ Source data EXISTS")
            else:
                logger.warning("    ✗ Source data is EMPTY")
        except Exception as e:
            logger.error(f"    ✗ Cannot query patients: {e}")
        
        # Check analytics tables
        logger.info("\n[8] Target data verification:")
        
        try:
            target_cur.execute("SELECT COUNT(*) FROM analytics.dim_patient")
            dim_count = target_cur.fetchone()[0]
            logger.info(f"    analytics.dim_patient: {dim_count} rows")
            
            if dim_count > 0:
                logger.info("    ✓ Data HAS BEEN LOADED")
            else:
                logger.warning("    ✗ Data NOT LOADED - ETL needs to run!")
        except Exception as e:
            logger.error(f"    ✗ Cannot query dim_patient: {e}")
        
        # Recommendation
        logger.info("\n[9] Recommendations:")
        
        try:
            source_cur.execute("SELECT COUNT(*) FROM operational.patients")
            src_count = source_cur.fetchone()[0]
            
            target_cur.execute("SELECT COUNT(*) FROM analytics.dim_patient")
            tgt_count = target_cur.fetchone()[0]
            
            if src_count > 0 and tgt_count == 0:
                logger.info("\n    ➤ SOURCE HAS DATA BUT TARGET IS EMPTY")
                logger.info("    ➤ Run ETL to load data:")
                logger.info("       python etl_analytics.py --mode incremental")
                logger.info("    ➤ Or run with verbose mode to see details:")
                logger.info("       python etl_analytics.py --mode incremental --verbose")
            
            elif src_count == 0:
                logger.info("\n    ➤ SOURCE DATABASE IS EMPTY")
                logger.info("    ➤ Add test data first:")
                logger.info("       INSERT INTO operational.patients (...) VALUES (...);")
                logger.info("    ➤ Then run ETL:")
                logger.info("       python etl_analytics.py --mode incremental")
            
            elif tgt_count > 0:
                logger.info("\n    ✓ EVERYTHING IS WORKING!")
                logger.info("    ✓ Source has data")
                logger.info("    ✓ Target has been populated")
                logger.info("    ✓ Run analytics_utils.py to validate")
        
        except Exception as e:
            logger.error(f"Error in recommendations: {e}")
        
        source_cur.close()
        target_cur.close()
        source_conn.close()
        target_conn.close()
        
        return True
    
    except Exception as e:
        logger.error(f"Error in diagnosis: {e}")
        return False

if __name__ == "__main__":
    print("\n" + "="*70)
    print("eFICHE ANALYTICS - DIAGNOSTIC REPORT")
    print("="*70 + "\n")
    
    check_source_database()
    check_target_database()
    compare_and_diagnose()
    
    print("\n" + "="*70)
    print("Diagnostic complete!")
    print("="*70 + "\n")