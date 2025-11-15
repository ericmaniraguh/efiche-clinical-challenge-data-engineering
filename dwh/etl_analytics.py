"""
eFICHE Analytics ETL Pipeline - TWO DATABASE ARCHITECTURE - FIXED
Explicitly reads from source database operational schema
Writes to target database analytics schema

Architecture:
  Source: DB_NAME (efiche_clinical_database) - operational.* tables
  Target: DB_ANALYTICS_NAME (efiche_clinical_db_analytics) - analytics.* tables
  
Features:
  * Incremental and full refresh modes
  * Dimension and fact table loading
  * Master data synchronization
  * Error handling and logging
  * Summary statistics reporting
  * Performance optimized

Usage:
  python etl_analytics.py --mode incremental
  python etl_analytics.py --mode full --verbose
"""

import sys
import os
import logging
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional
from datetime import datetime, timedelta
import argparse

# ============================================================================
# PATH RESOLUTION - DO THIS FIRST
# ============================================================================

def setup_paths():
    """Configure Python path for imports"""
    current_file = Path(__file__).resolve()
    current_dir = current_file.parent
    
    if 'dwh' in str(current_dir):
        project_root = current_dir.parent
    else:
        project_root = current_dir.parent if current_dir.name != 'dags' else current_dir
    
    paths_to_add = [
        str(project_root / 'utils'),
        str(project_root),
        str(current_dir),
        str(project_root / 'dwh'),
    ]
    
    for path in paths_to_add:
        if path and path not in sys.path:
            sys.path.insert(0, path)
    
    print(f"[INFO] Project root: {project_root}")
    return project_root


PROJECT_ROOT = setup_paths()

# ============================================================================
# IMPORT DEPENDENCIES
# ============================================================================

try:
    import psycopg2
    from psycopg2 import sql
except ImportError as e:
    print(f"[ERROR] Missing psycopg2: {e}")
    sys.exit(1)

try:
    from db_connection import (
        get_db_connection,
        get_db_config,
        init_logger,
        execute_query
    )
    print("[INFO] Successfully imported db_connection module")
except ImportError as e:
    print(f"[ERROR] Failed to import db_connection: {e}")
    sys.exit(1)

logger = init_logger('analytics_etl', logging.INFO)


# ============================================================================
# TWO-DATABASE ETL CLASS - FIXED
# ============================================================================

class TwoDatabaseAnalyticsETL:
    """
    Analytics ETL with two-database architecture - FIXED VERSION
    
    Reads from: DB_NAME (operational database)
    Writes to: DB_ANALYTICS_NAME (analytics warehouse)
    
    KEY FIX: Explicitly uses separate connections and reads from source DB
    """
    
    def __init__(self, mode: str = 'incremental'):
        """Initialize Two-Database Analytics ETL
        
        Args:
            mode: 'incremental' or 'full'
        """
        self.mode = mode
        
        # Get database names from .env
        self.source_db = os.getenv('DB_NAME', 'efiche_clinical_database')
        self.target_db = os.getenv('DB_ANALYTICS_NAME', 'efiche_clinical_db_analytics')
        
        self.source_conn = None
        self.target_conn = None
        
        self.stats = {
            'dimensions_loaded': {},
            'facts_loaded': {},
            'errors': [],
            'start_time': datetime.now(),
            'end_time': None
        }
        
        logger.info(f"Analytics ETL initialized - Mode: {mode}")
        logger.info(f"Source database (read): {self.source_db}")
        logger.info(f"Target database (write): {self.target_db}")
    
    def get_source_connection(self):
        """Get connection to source (operational) database"""
        try:
            if self.source_conn is None or self.source_conn.closed:
                self.source_conn = get_db_connection(db_name=self.source_db)
                logger.debug("Source database connection established")
            return self.source_conn
        except Exception as e:
            logger.error(f"Failed to connect to source database: {e}")
            raise
    
    def get_target_connection(self):
        """Get connection to target (analytics) database"""
        try:
            if self.target_conn is None or self.target_conn.closed:
                self.target_conn = get_db_connection(db_name=self.target_db)
                logger.debug("Target database connection established")
            return self.target_conn
        except Exception as e:
            logger.error(f"Failed to connect to target database: {e}")
            raise
    
    def close_connections(self):
        """Close both database connections"""
        try:
            if self.source_conn and not self.source_conn.closed:
                self.source_conn.close()
                logger.debug("Source database connection closed")
        except Exception as e:
            logger.warning(f"Warning closing source connection: {e}")
        
        try:
            if self.target_conn and not self.target_conn.closed:
                self.target_conn.close()
                logger.debug("Target database connection closed")
        except Exception as e:
            logger.warning(f"Warning closing target connection: {e}")
    
    # ========================================================================
    # DIMENSION LOADING - FIXED TO USE SEPARATE CONNECTIONS
    # ========================================================================
    
    def load_dim_patient(self) -> int:
        """Load patient dimension from source to target"""
        logger.info("Loading dim_patient...")
        
        try:
            source_conn = self.get_source_connection()
            target_conn = self.get_target_connection()
            
            source_cur = source_conn.cursor()
            target_cur = target_conn.cursor()
            
            if self.mode == 'full':
                target_cur.execute("TRUNCATE TABLE analytics.dim_patient CASCADE")
                logger.info("  Truncated dim_patient")
            
            # EXTRACT from source
            logger.debug("  Extracting from operational.patients...")
            source_cur.execute("""
                SELECT 
                    patient_id, patient_code, sex, date_of_birth, age,
                    geographic_location, first_encounter_date, last_encounter_date,
                    total_encounters
                FROM operational.patients
            """)
            
            rows_extracted = source_cur.rowcount
            logger.debug(f"  Extracted {rows_extracted:,} rows from source")
            
            # LOAD to target
            logger.debug("  Loading to analytics.dim_patient...")
            rows_loaded = 0
            for row in source_cur:
                target_cur.execute("""
                    INSERT INTO analytics.dim_patient (
                        patient_id, patient_code, sex, date_of_birth, age,
                        geographic_location, first_encounter_date, last_encounter_date, 
                        total_encounters, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (patient_id) DO UPDATE SET
                        age = EXCLUDED.age,
                        last_encounter_date = EXCLUDED.last_encounter_date,
                        total_encounters = EXCLUDED.total_encounters,
                        updated_at = CURRENT_TIMESTAMP
                """, row)
                rows_loaded += 1
            
            target_conn.commit()
            
            logger.info(f"  ✓ Loaded {rows_loaded:,} patient records")
            self.stats['dimensions_loaded']['dim_patient'] = rows_loaded
            return rows_loaded
        
        except Exception as e:
            logger.error(f"  ✗ Error loading dim_patient: {e}")
            self.stats['errors'].append(f"dim_patient: {str(e)}")
            if target_conn:
                try:
                    target_conn.rollback()
                except:
                    pass
            raise
    
    def load_dim_facility(self) -> int:
        """Load facility dimension from source to target"""
        logger.info("Loading dim_facility...")
        
        try:
            source_conn = self.get_source_connection()
            target_conn = self.get_target_connection()
            
            source_cur = source_conn.cursor()
            target_cur = target_conn.cursor()
            
            if self.mode == 'full':
                target_cur.execute("TRUNCATE TABLE analytics.dim_facility CASCADE")
                logger.info("  Truncated dim_facility")
            
            # Extract from source
            source_cur.execute("""
                SELECT 
                    facility_id, facility_code, facility_name, location,
                    region, country
                FROM master.facility_master
            """)
            
            # Load to target
            rows_loaded = 0
            for row in source_cur:
                target_cur.execute("""
                    INSERT INTO analytics.dim_facility (
                        facility_id, facility_code, facility_name, location, 
                        region, country, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (facility_id) DO UPDATE SET
                        facility_name = EXCLUDED.facility_name,
                        location = EXCLUDED.location,
                        region = EXCLUDED.region,
                        country = EXCLUDED.country,
                        updated_at = CURRENT_TIMESTAMP
                """, row)
                rows_loaded += 1
            
            target_conn.commit()
            
            logger.info(f"  ✓ Loaded {rows_loaded:,} facility records")
            self.stats['dimensions_loaded']['dim_facility'] = rows_loaded
            return rows_loaded
        
        except Exception as e:
            logger.error(f"  ✗ Error loading dim_facility: {e}")
            self.stats['errors'].append(f"dim_facility: {str(e)}")
            if target_conn:
                try:
                    target_conn.rollback()
                except:
                    pass
            raise
    
    def load_dim_modality(self) -> int:
        """Load modality dimension from source to target"""
        logger.info("Loading dim_modality...")
        
        try:
            source_conn = self.get_source_connection()
            target_conn = self.get_target_connection()
            
            source_cur = source_conn.cursor()
            target_cur = target_conn.cursor()
            
            if self.mode == 'full':
                target_cur.execute("TRUNCATE TABLE analytics.dim_modality CASCADE")
                logger.info("  Truncated dim_modality")
            
            # Extract from source
            source_cur.execute("""
                SELECT modality_id, modality_code, modality_name
                FROM master.modality_master
            """)
            
            # Load to target
            rows_loaded = 0
            for row in source_cur:
                target_cur.execute("""
                    INSERT INTO analytics.dim_modality (
                        modality_id, modality_code, modality_name, updated_at
                    )
                    VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (modality_id) DO UPDATE SET
                        modality_name = EXCLUDED.modality_name,
                        updated_at = CURRENT_TIMESTAMP
                """, row)
                rows_loaded += 1
            
            target_conn.commit()
            
            logger.info(f"  ✓ Loaded {rows_loaded:,} modality records")
            self.stats['dimensions_loaded']['dim_modality'] = rows_loaded
            return rows_loaded
        
        except Exception as e:
            logger.error(f"  ✗ Error loading dim_modality: {e}")
            self.stats['errors'].append(f"dim_modality: {str(e)}")
            if target_conn:
                try:
                    target_conn.rollback()
                except:
                    pass
            raise
    
    def load_dim_diagnosis(self) -> int:
        """Load diagnosis dimension from source to target"""
        logger.info("Loading dim_diagnosis...")
        
        try:
            source_conn = self.get_source_connection()
            target_conn = self.get_target_connection()
            
            source_cur = source_conn.cursor()
            target_cur = target_conn.cursor()
            
            if self.mode == 'full':
                target_cur.execute("TRUNCATE TABLE analytics.dim_diagnosis CASCADE")
                logger.info("  Truncated dim_diagnosis")
            
            # Extract from source
            source_cur.execute("""
                SELECT diagnosis_id, diagnosis_code, diagnosis_name, category
                FROM master.diagnosis_master
            """)
            
            # Load to target
            rows_loaded = 0
            for row in source_cur:
                target_cur.execute("""
                    INSERT INTO analytics.dim_diagnosis (
                        diagnosis_id, diagnosis_code, diagnosis_name, category, updated_at
                    )
                    VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (diagnosis_id) DO UPDATE SET
                        diagnosis_name = EXCLUDED.diagnosis_name,
                        category = EXCLUDED.category,
                        updated_at = CURRENT_TIMESTAMP
                """, row)
                rows_loaded += 1
            
            target_conn.commit()
            
            logger.info(f"  ✓ Loaded {rows_loaded:,} diagnosis records")
            self.stats['dimensions_loaded']['dim_diagnosis'] = rows_loaded
            return rows_loaded
        
        except Exception as e:
            logger.error(f"  ✗ Error loading dim_diagnosis: {e}")
            self.stats['errors'].append(f"dim_diagnosis: {str(e)}")
            if target_conn:
                try:
                    target_conn.rollback()
                except:
                    pass
            raise
    
    # ========================================================================
    # FACT TABLE LOADING
    # ========================================================================
    
    def load_fact_procedure(self, hours: int = 24) -> int:
        """Load procedure facts from source to target"""
        logger.info("Loading fact_procedure...")
        
        try:
            source_conn = self.get_source_connection()
            target_conn = self.get_target_connection()
            
            source_cur = source_conn.cursor()
            target_cur = target_conn.cursor()
            
            if self.mode == 'full':
                target_cur.execute("TRUNCATE TABLE analytics.fact_procedure CASCADE")
                logger.info("  Truncated fact_procedure")
                cutoff_clause = ""
            else:
                cutoff_time = datetime.now() - timedelta(hours=hours)
                cutoff_clause = f"AND e.encounter_date >= '{cutoff_time.isoformat()}'"
            
            # Extract operational data from source
            query = f"""
            SELECT 
                e.encounter_id,
                pr.procedure_id,
                e.patient_id,
                e.facility_id,
                pr.modality_id,
                pd.diagnosis_id,
                e.encounter_date,
                pm.projection_name,
                arm.region_name,
                fi.finding_severity,
                fi.abnormality_detected,
                fi.confidence_score,
                COUNT(DISTINCT ri.image_id)::INT as total_images,
                COALESCE(cr.word_count, 0) as report_word_count
            FROM operational.encounters e
            JOIN operational.procedures pr ON e.encounter_id = pr.encounter_id
            LEFT JOIN master.projection_master pm ON pr.projection_id = pm.projection_id
            LEFT JOIN master.anatomical_region_master arm ON pr.region_id = arm.region_id
            LEFT JOIN operational.findings fi ON pr.procedure_id = fi.procedure_id
            LEFT JOIN operational.procedure_diagnosis pd ON pr.procedure_id = pd.procedure_id 
                AND pd.is_primary = true
            LEFT JOIN operational.radiological_images ri ON pr.procedure_id = ri.procedure_id
            LEFT JOIN operational.clinical_reports cr ON pr.procedure_id = cr.procedure_id
            WHERE e.encounter_id IS NOT NULL
            {cutoff_clause}
            GROUP BY 
                e.encounter_id, pr.procedure_id, e.patient_id, e.facility_id, 
                pr.modality_id, pd.diagnosis_id, e.encounter_date, pm.projection_name, 
                arm.region_name, fi.finding_severity, fi.abnormality_detected, 
                fi.confidence_score, cr.word_count
            """
            
            source_cur.execute(query)
            
            # Load to target - now lookup dimension keys in target database
            rows_loaded = 0
            for row in source_cur:
                encounter_id, procedure_id, patient_id, facility_id, modality_id, diagnosis_id, \
                encounter_date, projection_name, region_name, finding_severity, abnormality_detected, \
                confidence_score, total_images, report_word_count = row
                
                # Lookup dimension keys from target database
                try:
                    target_cur.execute("SELECT patient_sk FROM analytics.dim_patient WHERE patient_id = %s", 
                                      (patient_id,))
                    patient_sk_result = target_cur.fetchone()
                    patient_sk = patient_sk_result[0] if patient_sk_result else -1
                    
                    target_cur.execute("SELECT facility_sk FROM analytics.dim_facility WHERE facility_id = %s",
                                      (facility_id,))
                    facility_sk_result = target_cur.fetchone()
                    facility_sk = facility_sk_result[0] if facility_sk_result else -1
                    
                    target_cur.execute("SELECT modality_sk FROM analytics.dim_modality WHERE modality_id = %s",
                                      (modality_id,))
                    modality_sk_result = target_cur.fetchone()
                    modality_sk = modality_sk_result[0] if modality_sk_result else -1
                    
                    diagnosis_sk = -1
                    if diagnosis_id:
                        target_cur.execute("SELECT diagnosis_sk FROM analytics.dim_diagnosis WHERE diagnosis_id = %s",
                                          (diagnosis_id,))
                        diagnosis_sk_result = target_cur.fetchone()
                        diagnosis_sk = diagnosis_sk_result[0] if diagnosis_sk_result else -1
                    
                    # Insert fact with looked-up dimension keys
                    target_cur.execute("""
                        INSERT INTO analytics.fact_procedure (
                            encounter_id, procedure_id, patient_sk, facility_sk, modality_sk, 
                            diagnosis_sk, encounter_date, projection_name, region_name, 
                            finding_severity, abnormality_detected, confidence_score, 
                            total_images, report_word_count, loaded_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                        ON CONFLICT (procedure_id) DO UPDATE SET
                            abnormality_detected = EXCLUDED.abnormality_detected,
                            confidence_score = EXCLUDED.confidence_score,
                            report_word_count = EXCLUDED.report_word_count,
                            loaded_at = CURRENT_TIMESTAMP
                    """, (encounter_id, procedure_id, patient_sk, facility_sk, modality_sk, 
                          diagnosis_sk, encounter_date, projection_name, region_name,
                          finding_severity, abnormality_detected, confidence_score,
                          total_images, report_word_count))
                    rows_loaded += 1
                
                except Exception as lookup_error:
                    logger.debug(f"  Warning: Could not insert procedure {procedure_id}: {lookup_error}")
                    continue
            
            target_conn.commit()
            
            logger.info(f"  ✓ Loaded {rows_loaded:,} procedure facts")
            self.stats['facts_loaded']['fact_procedure'] = rows_loaded
            return rows_loaded
        
        except Exception as e:
            logger.error(f"  ✗ Error loading fact_procedure: {e}")
            self.stats['errors'].append(f"fact_procedure: {str(e)}")
            if target_conn:
                try:
                    target_conn.rollback()
                except:
                    pass
            raise
    
    def load_fact_language_usage(self) -> int:
        """Load language usage facts from source to target"""
        logger.info("Loading fact_language_usage...")
        
        try:
            source_conn = self.get_source_connection()
            target_conn = self.get_target_connection()
            
            source_cur = source_conn.cursor()
            target_cur = target_conn.cursor()
            
            if self.mode == 'full':
                target_cur.execute("TRUNCATE TABLE analytics.fact_language_usage CASCADE")
                logger.info("  Truncated fact_language_usage")
            
            # Extract from source
            source_cur.execute("""
            SELECT 
                cr.report_id,
                cr.procedure_id,
                COALESCE(lr.language_code, 'UNKNOWN') as language_code,
                COALESCE(lr.language_name, 'Unknown') as language_name,
                COALESCE(lr_audio.language_code, NULL) as audio_language_code,
                COALESCE(lr_audio.language_name, NULL) as audio_language_name,
                COALESCE(cr.has_audio, false) as has_audio,
                COALESCE(cr.word_count, 0) as word_count
            FROM operational.clinical_reports cr
            LEFT JOIN master.language_registry lr ON cr.language_id = lr.language_id
            LEFT JOIN master.language_registry lr_audio ON cr.audio_language_id = lr_audio.language_id
            """)
            
            # Load to target
            rows_loaded = 0
            for row in source_cur:
                target_cur.execute("""
                    INSERT INTO analytics.fact_language_usage (
                        report_id, procedure_id, language_code, language_name,
                        audio_language_code, audio_language_name, has_audio, 
                        word_count, loaded_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (report_id) DO UPDATE SET
                        language_code = EXCLUDED.language_code,
                        language_name = EXCLUDED.language_name,
                        audio_language_code = EXCLUDED.audio_language_code,
                        audio_language_name = EXCLUDED.audio_language_name,
                        has_audio = EXCLUDED.has_audio,
                        word_count = EXCLUDED.word_count,
                        loaded_at = CURRENT_TIMESTAMP
                """, row)
                rows_loaded += 1
            
            target_conn.commit()
            
            logger.info(f"  ✓ Loaded {rows_loaded:,} language usage records")
            self.stats['facts_loaded']['fact_language_usage'] = rows_loaded
            return rows_loaded
        
        except Exception as e:
            logger.error(f"  ✗ Error loading fact_language_usage: {e}")
            self.stats['errors'].append(f"fact_language_usage: {str(e)}")
            if target_conn:
                try:
                    target_conn.rollback()
                except:
                    pass
            raise
    
    # ========================================================================
    # ORCHESTRATION
    # ========================================================================
    
    def run(self) -> Dict[str, Any]:
        """Run complete analytics ETL pipeline"""
        logger.info("="*75)
        logger.info("STARTING ANALYTICS ETL PIPELINE - TWO DATABASE ARCHITECTURE")
        logger.info("="*75)
        logger.info(f"Mode: {self.mode.upper()}")
        logger.info(f"Reading FROM:  {self.source_db} (operational)")
        logger.info(f"Writing TO:    {self.target_db} (analytics warehouse)")
        logger.info(f"Timestamp: {self.stats['start_time']}")
        logger.info("="*75)
        
        try:
            # Load dimensions first
            logger.info("\n[STAGE 1] Loading Dimensions...")
            self.load_dim_patient()
            self.load_dim_facility()
            self.load_dim_modality()
            self.load_dim_diagnosis()
            
            # Load facts
            logger.info("\n[STAGE 2] Loading Facts...")
            self.load_fact_procedure()
            self.load_fact_language_usage()
            
            self.stats['end_time'] = datetime.now()
            duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
            
            # Report
            logger.info("\n" + "="*75)
            logger.info("ANALYTICS ETL PIPELINE COMPLETE - SUMMARY STATISTICS")
            logger.info("="*75)
            
            logger.info(f"\n DIMENSIONS LOADED:")
            for dim, count in self.stats['dimensions_loaded'].items():
                logger.info(f"   {dim}: {count:,} rows")
            
            logger.info(f"\n FACTS LOADED:")
            for fact, count in self.stats['facts_loaded'].items():
                logger.info(f"   {fact}: {count:,} rows")
            
            total_dimensions = sum(self.stats['dimensions_loaded'].values())
            total_facts = sum(self.stats['facts_loaded'].values())
            total_loaded = total_dimensions + total_facts
            
            logger.info(f"\n SUMMARY STATISTICS:")
            logger.info(f"   Total dimension records: {total_dimensions:,}")
            logger.info(f"   Total fact records: {total_facts:,}")
            logger.info(f"   Total records loaded: {total_loaded:,}")
            logger.info(f"   Duration: {duration:.2f} seconds")
            logger.info(f"   Throughput: {total_loaded/duration:,.0f} records/second")
            
            if self.stats['errors']:
                logger.warning(f"\n⚠️  ERRORS ENCOUNTERED: {len(self.stats['errors'])}")
                for error in self.stats['errors']:
                    logger.warning(f"   {error}")
            else:
                logger.info(f"\n NO ERRORS - ETL SUCCESSFUL!")
            
            logger.info("\n" + "="*75)
            
            return self.stats
        
        except Exception as e:
            logger.error(f"\n Pipeline failed: {e}", exc_info=True)
            self.stats['error'] = str(e)
            self.stats['end_time'] = datetime.now()
            return self.stats
        
        finally:
            self.close_connections()


# ============================================================================
# AIRFLOW INTEGRATION
# ============================================================================

def populate_analytics_warehouse(mode: str = 'incremental', **kwargs) -> Dict[str, Any]:
    """Airflow-compatible function for analytics warehouse population"""
    etl = TwoDatabaseAnalyticsETL(mode=mode)
    return etl.run()


# ============================================================================
# COMMAND LINE INTERFACE
# ============================================================================

def main():
    """Command-line interface"""
    parser = argparse.ArgumentParser(
        description='eFiche Analytics ETL Pipeline - Two Database Architecture',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Two-Database Architecture:
  Source (reads from):  DB_NAME (efiche_clinical_database)
  Target (writes to):   DB_ANALYTICS_NAME (efiche_clinical_db_analytics)

Examples:
  python etl_analytics.py --mode incremental
  python etl_analytics.py --mode full
  python etl_analytics.py --mode incremental --verbose
        """
    )
    
    parser.add_argument(
        '--mode',
        choices=['incremental', 'full'],
        default='incremental',
        help='ETL mode (default: incremental)'
    )
    parser.add_argument(
        '--verbose',
        '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    try:
        etl = TwoDatabaseAnalyticsETL(mode=args.mode)
        stats = etl.run()
        
        if 'error' in stats:
            sys.exit(1)
        else:
            sys.exit(0)
    
    except KeyboardInterrupt:
        logger.info("ETL interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()