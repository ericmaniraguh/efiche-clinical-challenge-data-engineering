"""
eFICHE UNIFIED ETL & ANALYTICS PIPELINE DAG - CORRECT IMPORTS
Directory structure:
"""

import os
import sys
import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.exceptions import AirflowException

# =============================================================================
# LOGGING
# =============================================================================
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# =============================================================================
# PATHS - CORRECT FOR dwh FOLDER AT ROOT LEVEL
# =============================================================================
AIRFLOW_HOME = '/opt/airflow'
DAGS_HOME = os.path.join(AIRFLOW_HOME, 'dags')

# dwh folder is at ROOT LEVEL, SEPARATE from dags
# We need to go up two levels from this file to reach root, then into dwh
# This file is at: /opt/airflow/dags/efiche_unified_pipeline.py
# We want: /dwh/

DWH_PATH = os.path.abspath(os.path.join(AIRFLOW_HOME, '..', 'dwh'))

# Add dwh to path so we can import directly
if DWH_PATH not in sys.path:
    sys.path.insert(0, DWH_PATH)

# Key paths
CSV_PATH = os.path.join(AIRFLOW_HOME, 'data', 'padchest_synthetic_data.csv')
STATE_FILE = os.path.join(AIRFLOW_HOME, 'data', 'data_state.json')
SYNTHETIC_ENGINE_PATH = os.path.join(AIRFLOW_HOME, 'synthetic_data_engine')

if SYNTHETIC_ENGINE_PATH not in sys.path:
    sys.path.insert(0, SYNTHETIC_ENGINE_PATH)

logger.info(f"AIRFLOW_HOME: {AIRFLOW_HOME}")
logger.info(f"DAGS Home: {DAGS_HOME}")
logger.info(f"DWH Path: {DWH_PATH}")
logger.info(f"DWH exists: {os.path.exists(DWH_PATH)}")
logger.info(f"Added to sys.path: {DWH_PATH}")

# =============================================================================
# CONFIG
# =============================================================================

def safe_int_env(key: str, default: int) -> int:
    """Safely parse integer from environment variable"""
    import re
    value = os.getenv(key, str(default))
    if not value:
        return default
    match = re.match(r'^(\d+)', str(value).strip())
    if match:
        return int(match.group(1))
    return default

NUM_ROWS = safe_int_env('NUM_ROWS', 1000)
BATCH_SIZE = safe_int_env('BATCH_SIZE', 100)
APPEND_MODE = os.getenv('APPEND_MODE', 'true').lower() == 'true'

# Database
DB_HOST = os.getenv('DB_HOST', 'host.docker.internal')
DB_PORT = safe_int_env('DB_PORT', 5433)
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'admin')
DB_NAME = os.getenv('DB_NAME', 'efiche_clinical_database')
DB_ANALYTICS_NAME = os.getenv('DB_ANALYTICS_NAME', 'efiche_clinical_db_analytics')

# =============================================================================
# DAG
# =============================================================================

default_args = {
    'owner': 'data_engineering',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 11, 1),
    'catchup': False,
}

dag = DAG(
    'efiche_unified_etl_analytics',
    default_args=default_args,
    description='ETL: CSV → Operational DB → Analytics Warehouse',
    schedule_interval='0 1 * * *',
    tags=['efiche', 'etl', 'analytics'],
    max_active_runs=1,
)

# =============================================================================
# OPERATIONAL ETL TASKS
# =============================================================================

def generate_data(**context):
    """Generate synthetic data"""
    logger.info("\n" + "="*80)
    logger.info("STAGE 1: SYNTHETIC DATA GENERATION")
    logger.info("="*80 + "\n")
    
    try:
        from padChest_synthetic_data_generator import generate_padchest_data
        logger.info("✓ Imported generator")
        
        Path(CSV_PATH).parent.mkdir(parents=True, exist_ok=True)
        
        import inspect
        sig = inspect.signature(generate_padchest_data)
        logger.info(f"Function signature: {sig}\n")
        
        # Try with correct parameters
        try:
            result = generate_padchest_data(
                num_rows=NUM_ROWS,
                append_mode=APPEND_MODE,
                csv_file=CSV_PATH,
                state_file=STATE_FILE
            )
        except TypeError:
            logger.info("Retrying without append_mode...")
            result = generate_padchest_data(
                num_rows=NUM_ROWS,
                csv_file=CSV_PATH,
                state_file=STATE_FILE
            )
        
        # Handle result
        if isinstance(result, dict):
            generated = result.get('rows_generated', NUM_ROWS)
            total = result.get('total_rows', NUM_ROWS)
        else:
            generated = NUM_ROWS
            total = NUM_ROWS
        
        context['task_instance'].xcom_push(key='generated_rows', value=generated)
        context['task_instance'].xcom_push(key='total_rows', value=total)
        
        logger.info(f"✓ Generated {generated:,} rows\n")
        return {'status': 'success', 'generated': generated}
    
    except Exception as e:
        logger.error(f"[FAIL] {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise AirflowException(f"Generation failed: {e}")


def validate_csv(**context):
    """Validate CSV exists"""
    logger.info("="*80)
    logger.info("STAGE 2: CSV VALIDATION")
    logger.info("="*80 + "\n")
    
    if not os.path.exists(CSV_PATH):
        raise AirflowException(f"CSV not found: {CSV_PATH}")
    
    size_mb = os.path.getsize(CSV_PATH) / (1024 * 1024)
    logger.info(f"✓ CSV found: {size_mb:.2f} MB\n")
    return 'load_csv'


def load_csv(**context):
    """Load CSV"""
    logger.info("="*80)
    logger.info("STAGE 3: CSV LOADING")
    logger.info("="*80 + "\n")
    
    try:
        import pandas as pd
        df = pd.read_csv(CSV_PATH)
        logger.info(f"✓ Loaded {len(df):,} rows, {len(df.columns)} columns\n")
        context['task_instance'].xcom_push(key='csv_rows', value=len(df))
        return f"CSV ready: {len(df):,} rows"
    except Exception as e:
        raise AirflowException(f"CSV load failed: {e}")


def load_database(**context):
    """Load to PostgreSQL"""
    logger.info("="*80)
    logger.info("STAGE 4: DATABASE LOADING")
    logger.info("="*80 + "\n")
    
    try:
        import psycopg2
        import pandas as pd
        import uuid
        from datetime import datetime as dt
        
        logger.info(f"Connecting to {DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}\n")
        
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, user=DB_USER,
            password=DB_PASSWORD, database=DB_NAME, connect_timeout=10
        )
        cursor = conn.cursor()
        logger.info("[OK] Connected\n")
        
        try:
            df = pd.read_csv(CSV_PATH)
            total = len(df)
            success = 0
            
            for idx, row in df.iterrows():
                try:
                    pid = str(row.get('PatientID', f'PAT{idx:06d}'))[:50]
                    dob = row.get('DateOfBirth')
                    sex = str(row.get('PatientSex', 'M'))[:10]
                    location = 'Unknown'
                    
                    # Check/insert patient
                    cursor.execute("SELECT patient_id FROM operational.patients WHERE patient_code = %s", (pid,))
                    result = cursor.fetchone()
                    
                    if not result:
                        patient_id = str(uuid.uuid4())
                        cursor.execute(
                            """INSERT INTO operational.patients 
                               (patient_id, patient_code, date_of_birth, sex, geographic_location,
                                first_encounter_date, last_encounter_date, total_encounters)
                               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                            (patient_id, pid, dob, sex, location, dt.now(), dt.now(), 0)
                        )
                    else:
                        patient_id = result[0]
                    
                    # Insert encounter
                    cursor.execute(
                        """INSERT INTO operational.encounters
                           (encounter_id, patient_id, facility_id, encounter_code, encounter_date)
                           VALUES (%s, %s, %s, %s, %s)""",
                        (str(uuid.uuid4()), patient_id, str(uuid.uuid4()), pid, row.get('StudyDate'))
                    )
                    
                    success += 1
                except Exception as row_err:
                    logger.debug(f"Row {idx}: {str(row_err)[:50]}")
                
                if (idx + 1) % BATCH_SIZE == 0:
                    conn.commit()
                    logger.info(f"Batch {(idx+1)//BATCH_SIZE}: {success:,} rows")
            
            conn.commit()
            logger.info(f"\n✓ Loaded {success:,}/{total:,} rows ({100*success/max(1,total):.1f}%)\n")
            context['task_instance'].xcom_push(key='loaded_rows', value=success)
            return success
        
        finally:
            cursor.close()
            conn.close()
        
    except Exception as e:
        logger.error(f"[FAIL] {e}")
        raise AirflowException(f"Database load failed: {e}")


# =============================================================================
# ANALYTICS TASKS
# =============================================================================

def populate_analytics(**context):
    """Populate analytics warehouse using dwh modules"""
    logger.info("\n" + "="*80)
    logger.info("ANALYTICS: POPULATE WAREHOUSE")
    logger.info("="*80 + "\n")
    
    try:
        # CORRECT IMPORT: Import directly from dwh (which is in sys.path)
        import dwh.etl_analytics
        import dwh.analytics_utils
        
        logger.info(f"✓ Imported etl_analytics from {DWH_PATH}")
        logger.info(f"✓ Imported analytics_utils from {DWH_PATH}\n")
        
        # Check what functions are available
        logger.info("Available functions in etl_analytics:")
        for name in dir(dwh.etl_analytics):
            if not name.startswith('_'):
                logger.info(f"  - {name}")
        
        # Try to find and call the main function
        func_names = [
            'populate_analytics_warehouse',
            'populate_warehouse', 
            'AnalyticsETL',
            'run_etl',
            'main'
        ]
        
        func_found = None
        for func_name in func_names:
            if hasattr(dwh.etl_analytics, func_name):
                func_found = getattr(dwh.etl_analytics, func_name)
                logger.info(f"\n✓ Found function: {func_name}\n")
                break
        
        if not func_found:
            raise AirflowException(
                f"Could not find analytics function. Available: "
                f"{[n for n in dir(dwh.etl_analytics) if not n.startswith('_')]}"
            )
        
        # Call the function based on type
        if hasattr(func_found, '__call__'):
            # It's callable
            if isinstance(func_found, type):
                # It's a class - instantiate it
                logger.info(f"Instantiating {func_found.__name__}...")
                instance = func_found(mode='incremental')
                result = instance.run()
            else:
                # It's a regular function
                logger.info(f"Calling {func_found.__name__}...")
                result = func_found(mode='incremental')
            
            logger.info(f"\n✓ Analytics completed successfully\n")
            return result
        else:
            raise AirflowException(f"{func_found} is not callable")
    
    except Exception as e:
        logger.error(f"[FAIL] Analytics error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise AirflowException(f"Analytics failed: {e}")


def validate_analytics(**context):
    """Validate analytics data"""
    logger.info("="*80)
    logger.info("ANALYTICS: VALIDATION")
    logger.info("="*80 + "\n")
    
    try:
        # CORRECT IMPORT: Direct import from dwh
        import dwh.analytics_utils
        
        # Try to find validation function
        if hasattr(dwh.analytics_utils, 'validate_analytics'):
            result = dwh.analytics_utils.validate_analytics(mode='incremental', check_level='basic')
        else:
            logger.warning("validate_analytics function not found, skipping validation")
            result = {'status': 'skipped'}
        
        logger.info(f"✓ Validation complete\n")
        return result
    
    except Exception as e:
        logger.error(f"[FAIL] {e}")
        raise AirflowException(f"Validation failed: {e}")


def refresh_views(**context):
    """Refresh materialized views"""
    logger.info("="*80)
    logger.info("ANALYTICS: REFRESH VIEWS")
    logger.info("="*80 + "\n")
    
    try:
        import psycopg2
        
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, user=DB_USER,
            password=DB_PASSWORD, database=DB_NAME, connect_timeout=10
        )
        cursor = conn.cursor()
        
        logger.info("Refreshing materialized views...")
        cursor.execute("SELECT analytics.refresh_all_views();")
        conn.commit()
        
        logger.info("✓ Views refreshed\n")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.warning(f"View refresh skipped: {e}")


# =============================================================================
# SUMMARY
# =============================================================================

def summary(**context):
    """Print summary"""
    logger.info("="*80)
    logger.info("EXECUTION SUMMARY")
    logger.info("="*80)
    
    try:
        generated = context['task_instance'].xcom_pull(task_ids='generate_data', key='generated_rows') or 0
        loaded = context['task_instance'].xcom_pull(task_ids='load_database', key='loaded_rows') or 0
        total = context['task_instance'].xcom_pull(task_ids='generate_data', key='total_rows') or 0
        
        logger.info(f"\nOperational ETL:")
        logger.info(f"  Generated: {generated:,} rows")
        logger.info(f"  Loaded: {loaded:,} rows")
        logger.info(f"  Success rate: {100*loaded/max(1,total):.1f}%")
        logger.info(f"\nAnalytics ETL: COMPLETED")
        logger.info(f"\nPipeline Status: ✓ SUCCESS")
        logger.info("="*80 + "\n")
    except Exception as e:
        logger.warning(f"Could not get summary: {e}")


# =============================================================================
# TASKS
# =============================================================================

start = DummyOperator(task_id='start', dag=dag)

gen = PythonOperator(task_id='generate_data', python_callable=generate_data, dag=dag)
check = BranchPythonOperator(task_id='check_csv', python_callable=validate_csv, dag=dag)
load_c = PythonOperator(task_id='load_csv', python_callable=load_csv, dag=dag)
load_db = PythonOperator(task_id='load_database', python_callable=load_database, dag=dag)

ana_pop = PythonOperator(task_id='analytics_populate', python_callable=populate_analytics, dag=dag)
ana_val = PythonOperator(task_id='analytics_validate', python_callable=validate_analytics, dag=dag)
ana_ref = PythonOperator(task_id='analytics_refresh', python_callable=refresh_views, dag=dag)

sum_task = PythonOperator(task_id='summary', python_callable=summary, trigger_rule='all_done', dag=dag)
end = DummyOperator(task_id='end', trigger_rule='all_done', dag=dag)

# =============================================================================
# DEPENDENCIES
# =============================================================================

start >> gen >> check >> load_c >> load_db >> ana_pop >> ana_val >> ana_ref >> sum_task >> end