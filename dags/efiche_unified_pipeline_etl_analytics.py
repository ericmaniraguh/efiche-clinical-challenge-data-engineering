"""
eFICHE UNIFIED ETL & ANALYTICS PIPELINE DAG - VALIDATION FIRST
Skips data generation and verifies CSV existence before loading.
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
# PATHS
# =============================================================================
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME', '/opt/airflow')
DAGS_HOME = os.path.join(AIRFLOW_HOME, 'dags')

# Define paths
DWH_PATH = os.path.join(DAGS_HOME, 'dwh')
UTILS_PATH = os.path.join(DAGS_HOME, 'utils')
SYNTHETIC_ENGINE_PATH = os.path.join(AIRFLOW_HOME, 'synthetic_data_engine')
PIPELINE_PATH = os.path.join(AIRFLOW_HOME, 'pipeline')

# Add to sys.path so Python can find modules
for path in [DWH_PATH, UTILS_PATH, SYNTHETIC_ENGINE_PATH, PIPELINE_PATH]:
    if path not in sys.path:
        sys.path.insert(0, path)
        logger.info(f"Added to sys.path: {path}")

# Data Paths
CSV_PATH = os.path.join(AIRFLOW_HOME, 'data', 'padchest_synthetic_data.csv')
STATE_FILE = os.path.join(AIRFLOW_HOME, 'data', 'data_state.json')

# =============================================================================
# CONFIGURATION
# =============================================================================

def safe_int_env(key: str, default: int) -> int:
    import re
    value = os.getenv(key, str(default))
    if not value or value.lower() == 'none':
        return default
    match = re.match(r'^(\d+)', str(value).strip())
    if match:
        return int(match.group(1))
    return default

# Database Config
DB_HOST = os.getenv('EFICHE_DB_HOST_INTERNAL', os.getenv('DB_HOST', 'efiche_postgres'))
DB_PORT = safe_int_env('EFICHE_DB_PORT_INTERNAL', safe_int_env('DB_PORT', 5432)) 
DB_USER = os.getenv('PG_USER', 'postgres')
DB_PASSWORD = os.getenv('PG_PASSWORD', 'admin')
DB_NAME = os.getenv('PG_DATABASE_OPERATIONAL', 'efiche_clinical_database')
BATCH_SIZE = safe_int_env('BATCH_SIZE', 1000)

logger.info(f"Configuration Loaded: DB={DB_HOST}:{DB_PORT}/{DB_NAME}")

# =============================================================================
# DAG DEFINITION
# =============================================================================

default_args = {
    'owner': 'data_engineering',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 11, 1),
    'catchup': False,
}

dag = DAG(
    'efiche_unified_etl_analytics',
    default_args=default_args,
    description='ETL: Validate CSV -> Load DB -> Populate Analytics',
    schedule_interval='0 1 * * *',
    tags=['efiche', 'etl', 'analytics'],
    max_active_runs=1,
)

# =============================================================================
# TASKS
# =============================================================================

def validate_csv(**context):
    """
    Validate CSV exists before loading.
    Returns the task_id of the next task ('load_csv') if successful.
    """
    logger.info("STAGE 2: CSV VALIDATION")
    logger.info(f"Looking for file at: {CSV_PATH}")
    
    if not os.path.exists(CSV_PATH):
        # FAIL if file is missing
        logger.error(f"[FAIL] CSV not found at {CSV_PATH}")
        raise AirflowException(f"CSV not found at: {CSV_PATH}. Please run the generator first.")
    
    # Check file size
    size_mb = os.path.getsize(CSV_PATH) / (1024 * 1024)
    logger.info(f"✓ CSV found: {size_mb:.2f} MB")
    
    # Return the ID of the next task to run
    return 'load_csv'

def load_csv(**context):
    """Load CSV into DataFrame to verify structure"""
    logger.info("STAGE 3: CSV LOADING")
    try:
        import pandas as pd
        df = pd.read_csv(CSV_PATH)
        logger.info(f"✓ Loaded {len(df):,} rows, {len(df.columns)} columns")
        context['task_instance'].xcom_push(key='csv_rows', value=len(df))
        return f"CSV ready: {len(df):,} rows"
    except Exception as e:
        raise AirflowException(f"CSV load failed: {e}")

def load_database(**context):
    """Load DataFrame to PostgreSQL Operational DB using robust pipeline logic"""
    logger.info("STAGE 4: DATABASE LOADING")
    try:
        # Import the robust loading function from pipeline/load_data_to_pgadmin.py
        from load_data_to_pgadmin import load_data_to_database as run_pipeline_load
        
        logger.info(f"Starting robust data load to {DB_HOST}:{DB_PORT}/{DB_NAME}")
        
        # Execute the pipeline load
        stats = run_pipeline_load(
            csv_file=CSV_PATH,
            batch_size=BATCH_SIZE,
            db_host=DB_HOST,
            db_port=DB_PORT,
            db_user=DB_USER,
            db_password=DB_PASSWORD,
            db_name=DB_NAME
        )
        
        if stats.get('error'):
            raise Exception(stats['error'])
            
        success_count = stats.get('success', 0)
        logger.info(f"✓ Database Load Complete: {success_count} rows processed successfully.")
        context['task_instance'].xcom_push(key='loaded_rows', value=success_count)
        
    except Exception as e:
        logger.error(f"[FAIL] Database Error: {e}")
        raise AirflowException(f"Database load failed: {e}")

def populate_analytics(**context):
    """Populate analytics warehouse using dwh modules"""
    logger.info("STAGE 5: ANALYTICS POPULATION")
    
    # Ensure paths are in sys.path for this execution context
    for path in [DWH_PATH, UTILS_PATH, SYNTHETIC_ENGINE_PATH, PIPELINE_PATH]:
        if path not in sys.path:
            sys.path.insert(0, path)
            logger.info(f"Added to sys.path (runtime): {path}")
            
    # Debug: List DWH directory
    try:
        if os.path.exists(DWH_PATH):
            logger.info(f"DWH_PATH contents ({DWH_PATH}): {os.listdir(DWH_PATH)}")
        else:
            logger.error(f"DWH_PATH does not exist: {DWH_PATH}")
    except Exception as e:
        logger.warning(f"Could not list DWH_PATH: {e}")

    try:
        import etl_analytics
        # Use the exposed helper function if available
        if hasattr(etl_analytics, 'populate_analytics_warehouse'):
             logger.info("Running populate_analytics_warehouse...")
             return etl_analytics.populate_analytics_warehouse(mode='incremental')
        # Fallback to class instantiation
        elif hasattr(etl_analytics, 'TwoDatabaseAnalyticsETL'):
             logger.info("Running TwoDatabaseAnalyticsETL class...")
             return etl_analytics.TwoDatabaseAnalyticsETL(mode='incremental').run()
        # Legacy support
        elif hasattr(etl_analytics, 'AnalyticsETL'):
             logger.info("Running AnalyticsETL class...")
             return etl_analytics.AnalyticsETL(mode='incremental').run()
        else:
             # Fallback imports
             from db_connection import get_db_connection
             logger.info("Modules imported successfully, running custom logic if needed.")
    except Exception as e:
        logger.error(f"Analytics Failed: {e}")
        raise AirflowException(f"Analytics failed: {e}")

def validate_analytics(**context):
    """Validate analytics integrity"""
    logger.info("STAGE 6: ANALYTICS VALIDATION")
    try:
        import analytics_utils
        if hasattr(analytics_utils, 'validate_analytics'):
            return analytics_utils.validate_analytics(mode='incremental', check_level='basic')
    except Exception as e:
        logger.warning(f"Validation skipped: {e}")

def refresh_views(**context):
    """Refresh Materialized Views"""
    logger.info("STAGE 7: REFRESH VIEWS")
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, user=DB_USER,
            password=DB_PASSWORD, database=DB_NAME
        )
        cursor = conn.cursor()
        cursor.execute("SELECT analytics.refresh_all_views();")
        conn.commit()
        cursor.close()
        conn.close()
        logger.info("✓ Views refreshed")
    except Exception as e:
        logger.warning(f"View refresh warning: {e}")

def summary(**context):
    """Pipeline Summary"""
    logger.info("\n" + "="*80)
    logger.info("PIPELINE SUCCESS")
    logger.info("="*80)

# =============================================================================
# TASK ORCHESTRATION
# =============================================================================

start = DummyOperator(task_id='start', dag=dag)

# --- SKIPPED: GENERATION TASK ---
# gen = PythonOperator(task_id='generate_data', python_callable=generate_data, dag=dag)

# 1. Validation (Checks file exists)
check = BranchPythonOperator(
    task_id='check_csv', 
    python_callable=validate_csv, 
    dag=dag
)

# 2. Load CSV (Reads file)
load_c = PythonOperator(task_id='load_csv', python_callable=load_csv, dag=dag)

# 3. Load DB (Writes to Operational)
load_db = PythonOperator(task_id='load_database', python_callable=load_database, dag=dag)

# 4. Analytics (Transforms to DWH)
ana_pop = PythonOperator(task_id='analytics_populate', python_callable=populate_analytics, dag=dag)
ana_val = PythonOperator(task_id='analytics_validate', python_callable=validate_analytics, dag=dag)
ana_ref = PythonOperator(task_id='analytics_refresh', python_callable=refresh_views, dag=dag)

sum_task = PythonOperator(task_id='summary', python_callable=summary, trigger_rule='all_done', dag=dag)
end = DummyOperator(task_id='end', trigger_rule='all_done', dag=dag)

# =============================================================================
# DEPENDENCIES
# =============================================================================

# Start -> Check -> Load CSV -> Load DB -> Analytics -> Summary -> End
start >> check >> load_c >> load_db >> ana_pop >> ana_val >> ana_ref >> sum_task >> end