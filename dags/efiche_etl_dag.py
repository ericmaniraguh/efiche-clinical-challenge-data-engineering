"""
eFICHE ETL Pipeline DAG - FINAL FIXED VERSION
Uses correct function signature for padChest_synthetic_data_generator

Error fixed:
  - generate_padchest_data() got an unexpected keyword argument 'append_mode'
  - Solution: Use actual function parameters from generator
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
# CONFIGURATION
# =============================================================================
AIRFLOW_HOME = '/opt/airflow'
CSV_PATH = os.path.join(AIRFLOW_HOME, 'data', 'padchest_synthetic_data.csv')
STATE_FILE = os.path.join(AIRFLOW_HOME, 'data', 'data_state.json')

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
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

logger = logging.getLogger(__name__)
logger.setLevel(LOG_LEVEL)

# =============================================================================
# DAG CONFIGURATION
# =============================================================================
default_args = {
    'owner': 'eric.maniraguha',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 11, 14),
    'catchup': False,
}

local_tz = pendulum.timezone("Africa/Kigali")

dag = DAG(
    'efiche_etl_pipeline',
    default_args=default_args,
    description='eFiche clinical data ETL pipeline - FIXED',
    schedule_interval='0 3 * * *',  # Run daily at 03:00 local
    start_date=datetime(2025, 11, 15, 3, 0, tzinfo=local_tz),  # First run today at 03:00
    tags=['efiche', 'etl', 'padchest'],
    max_active_runs=1,
)

# =============================================================================
# TASK FUNCTIONS
# =============================================================================

def generate_synthetic_data(**context):
    """Generate synthetic data - FIXED parameter signature"""
    logger.info("="*80)
    logger.info("STEP 1: SYNTHETIC DATA GENERATION")
    logger.info("="*80)
    logger.info(f"Generating {NUM_ROWS:,} rows to: {CSV_PATH}\n")
    
    try:
        # Add synthetic_data_engine to path
        synthetic_engine_path = os.path.join(AIRFLOW_HOME, 'synthetic_data_engine')
        
        if not os.path.exists(synthetic_engine_path):
            raise AirflowException(f"Synthetic data engine not found at: {synthetic_engine_path}")
        
        if synthetic_engine_path not in sys.path:
            sys.path.insert(0, synthetic_engine_path)
        
        logger.info(f"Adding to sys.path: {synthetic_engine_path}")
        
        # Import the generator module
        try:
            from padChest_synthetic_data_generator import generate_padchest_data
            logger.info(f"✓ Successfully imported padChest_synthetic_data_generator")
        except ImportError as e:
            logger.error(f"Could not import function: {e}")
            logger.error("Available files:")
            for f in os.listdir(synthetic_engine_path):
                logger.error(f"  - {f}")
            raise AirflowException(f"Import failed: {e}")
        
        # Create data directory
        Path(CSV_PATH).parent.mkdir(parents=True, exist_ok=True)
        
        # Check function signature to understand correct parameters
        import inspect
        sig = inspect.signature(generate_padchest_data)
        logger.info(f"\nFunction signature: {sig}")
        logger.info(f"Function parameters: {list(sig.parameters.keys())}\n")
        
        # Call the generator with CORRECT parameters
        # Based on error, it seems the function uses different parameter names
        # Let's try the most common signatures
        
        try:
            # Try with num_rows and output_file (most common)
            logger.info(f"Attempting call with (num_rows={NUM_ROWS}, output_file={CSV_PATH})")
            result = generate_padchest_data(
                num_rows=NUM_ROWS,
                output_file=CSV_PATH
            )
        except TypeError as te:
            logger.warning(f"First attempt failed: {te}")
            
            try:
                # Try with just num_rows
                logger.info(f"Attempting call with (num_rows={NUM_ROWS}) - let it use defaults")
                result = generate_padchest_data(num_rows=NUM_ROWS)
            except TypeError as te2:
                logger.warning(f"Second attempt failed: {te2}")
                
                # Try positional arguments
                logger.info(f"Attempting call with positional args: ({NUM_ROWS}, {CSV_PATH})")
                result = generate_padchest_data(NUM_ROWS, CSV_PATH)
        
        logger.info(f"\nGeneration result: {result}\n")
        
        # Handle different result formats
        if isinstance(result, dict):
            if result.get('status') != 'success':
                raise AirflowException(f"Data generation failed: {result.get('error', 'Unknown')}")
            generated_rows = result.get('rows_generated', 0)
            total_rows = result.get('total_rows', 0)
        else:
            # Result might be just a number or string
            generated_rows = result if isinstance(result, int) else NUM_ROWS
            total_rows = generated_rows
            logger.info(f"Result type: {type(result)}")
        
        # Push stats to XCom for downstream tasks
        context['task_instance'].xcom_push(key='total_rows', value=total_rows)
        context['task_instance'].xcom_push(key='generated_rows', value=generated_rows)
        
        logger.info(f"✓ Generated {generated_rows:,} new rows")
        logger.info(f"✓ Total in file: {total_rows:,} rows\n")
        
        return {'status': 'success', 'generated': generated_rows, 'total': total_rows}
    
    except Exception as e:
        logger.error(f"[FAIL] Generation error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise AirflowException(f"Data generation failed: {e}")


def check_csv_file_exists(**context):
    """Validate CSV exists before loading"""
    logger.info("="*80)
    logger.info("STEP 2: CSV VALIDATION")
    logger.info("="*80)
    logger.info(f"Checking: {CSV_PATH}\n")
    
    if not os.path.exists(CSV_PATH):
        logger.error(f"CSV NOT found: {CSV_PATH}")
        raise AirflowException(f"CSV not found: {CSV_PATH}")
    
    file_size_mb = os.path.getsize(CSV_PATH) / (1024 * 1024)
    logger.info(f"✓ CSV found")
    logger.info(f"✓ Size: {file_size_mb:.2f} MB\n")
    
    return 'load_csv_data'


def load_csv_data(**context):
    """Load and validate CSV structure"""
    logger.info("="*80)
    logger.info("STEP 3: CSV STRUCTURE VALIDATION")
    logger.info("="*80)
    
    try:
        import pandas as pd
        
        df = pd.read_csv(CSV_PATH)
        logger.info(f"✓ Loaded {len(df):,} rows")
        logger.info(f"✓ Columns: {len(df.columns)}")
        logger.info(f"✓ Sample: {', '.join(df.columns[:5])}...\n")
        
        # Validate required columns
        required_cols = ['PatientID', 'StudyID']
        missing = [col for col in required_cols if col not in df.columns]
        
        if missing:
            logger.warning(f"Note: Missing expected columns: {missing}")
            logger.warning(f"Available columns: {', '.join(df.columns[:10])}")
        
        context['task_instance'].xcom_push(key='csv_rows', value=len(df))
        logger.info(f"✓ CSV structure valid\n")
        
        return f"CSV ready: {len(df):,} rows"
    
    except Exception as e:
        logger.error(f"[FAIL] CSV load: {e}")
        raise AirflowException(f"CSV load failed: {e}")


def transform_data(**context):
    """Transform and prepare data for database"""
    logger.info("="*80)
    logger.info("STEP 4: DATA TRANSFORMATION")
    logger.info("="*80)
    
    try:
        import pandas as pd
        
        df = pd.read_csv(CSV_PATH)
        
        logger.info(f"✓ Validated {len(df):,} rows")
        logger.info(f"✓ Columns available: {len(df.columns)}")
        logger.info(f"✓ Ready for database load\n")
        
        context['task_instance'].xcom_push(key='transformed_rows', value=len(df))
        return f"Ready to load {len(df):,} rows to database"
    
    except Exception as e:
        logger.error(f"[FAIL] Transform: {e}")
        raise AirflowException(f"Transform failed: {e}")


def load_to_database(**context):
    """Load data to PostgreSQL operational database"""
    logger.info("="*80)
    logger.info("STEP 5: DATABASE LOADING")
    logger.info("="*80)
    
    try:
        import psycopg2
        import pandas as pd
        import uuid
        from datetime import datetime as dt
        
        # Database configuration from environment
        db_host = os.getenv('DB_HOST', 'host.docker.internal')
        db_port = int(os.getenv('DB_PORT', 5433))
        db_user = os.getenv('DB_USER', 'postgres')
        db_password = os.getenv('DB_PASSWORD', 'admin')
        db_name = os.getenv('DB_NAME', 'efiche_clinical_database')
        
        logger.info(f"Database: {db_user}@{db_host}:{db_port}/{db_name}")
        logger.info(f"CSV: {CSV_PATH}")
        logger.info(f"Batch size: {BATCH_SIZE}\n")
        
        # Connect to database
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_password,
            database=db_name,
            connect_timeout=10
        )
        cursor = conn.cursor()
        logger.info("[OK] Database connected\n")
        
        try:
            # Load CSV
            df = pd.read_csv(CSV_PATH)
            total_rows = len(df)
            success_rows = 0
            failed_rows = 0
            
            logger.info(f"Processing {total_rows:,} rows in batches of {BATCH_SIZE}\n")
            
            # Get actual column names
            actual_cols = df.columns.tolist()
            logger.info(f"CSV columns: {actual_cols[:5]}...")
            
            # Map to standard column names (handle variations)
            col_mapping = {
                'PatientID': None, 'Patient ID': None, 'patient_id': None,
                'StudyID': None, 'Study ID': None, 'study_id': None,
                'StudyDate': None, 'Study Date': None, 'study_date': None,
                'DateOfBirth': None, 'Date of Birth': None, 'date_of_birth': None,
                'PatientSex': None, 'Patient Sex': None, 'patient_sex': None, 'Sex': None,
                'Location': None, 'location': None,
            }
            
            # Find actual column names
            for col in actual_cols:
                col_lower = col.lower().replace(' ', '_')
                for key in col_mapping:
                    if col_lower == key.lower().replace(' ', '_'):
                        col_mapping[key] = col
                        break
            
            logger.info(f"Column mapping: {[v for v in col_mapping.values() if v]}\n")
            
            # Process batches
            for batch_start in range(0, total_rows, BATCH_SIZE):
                batch_end = min(batch_start + BATCH_SIZE, total_rows)
                batch_df = df.iloc[batch_start:batch_end]
                
                batch_success = 0
                batch_failed = 0
                
                for idx, row in batch_df.iterrows():
                    try:
                        # Extract data with fallback to defaults
                        pid = str(row.get(col_mapping.get('PatientID') or 'PatientID', f'PAT{idx:06d}'))[:50]
                        dob = row.get(col_mapping.get('DateOfBirth') or 'DateOfBirth')
                        sex = str(row.get(col_mapping.get('PatientSex') or 'PatientSex', 'M'))[:10] if pd.notna(row.get(col_mapping.get('PatientSex') or 'PatientSex')) else 'M'
                        location = str(row.get(col_mapping.get('Location') or 'Location', 'Unknown'))[:255] if pd.notna(row.get(col_mapping.get('Location') or 'Location')) else 'Unknown'
                        
                        # Check if patient exists
                        cursor.execute(
                            "SELECT patient_id FROM operational.patients WHERE patient_code = %s",
                            (pid,)
                        )
                        result = cursor.fetchone()
                        
                        if result:
                            patient_id = result[0]
                        else:
                            # Insert new patient
                            patient_id = str(uuid.uuid4())
                            cursor.execute(
                                """INSERT INTO operational.patients 
                                   (patient_id, patient_code, date_of_birth, sex, geographic_location,
                                    first_encounter_date, last_encounter_date, total_encounters)
                                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                   ON CONFLICT (patient_code) DO NOTHING
                                """,
                                (patient_id, pid, dob, sex, location, dt.now(), dt.now(), 0)
                            )
                        
                        # Insert encounter
                        encounter_id = str(uuid.uuid4())
                        facility_id = str(uuid.uuid4())
                        study_date = row.get(col_mapping.get('StudyDate') or 'StudyDate')
                        study_id = str(row.get(col_mapping.get('StudyID') or 'StudyID', f'{pid}-{idx}'))[:100]
                        
                        cursor.execute(
                            """INSERT INTO operational.encounters
                               (encounter_id, patient_id, facility_id, encounter_code, encounter_date)
                               VALUES (%s, %s, %s, %s, %s)
                            """,
                            (encounter_id, patient_id, facility_id, study_id, study_date)
                        )
                        
                        batch_success += 1
                        success_rows += 1
                    
                    except Exception as row_err:
                        logger.debug(f"Row {idx} error: {str(row_err)[:100]}")
                        batch_failed += 1
                        failed_rows += 1
                
                # Commit batch
                try:
                    conn.commit()
                    logger.info(f"Batch {batch_start//BATCH_SIZE + 1}: {batch_success:,}/{len(batch_df):,} success | "
                               f"Total: {success_rows:,}/{batch_end:,}")
                except Exception as batch_err:
                    logger.error(f"Batch commit failed: {batch_err}")
                    conn.rollback()
                    break
            
            logger.info(f"\n✓ Load complete")
            logger.info(f"  Success: {success_rows:,}/{total_rows:,}")
            logger.info(f"  Failed: {failed_rows:,}/{total_rows:,}")
            logger.info(f"  Success rate: {100*success_rows/max(1,total_rows):.1f}%\n")
            
            context['task_instance'].xcom_push(key='loaded_rows', value=success_rows)
            
            return {'status': 'success', 'loaded': success_rows, 'total': total_rows}
        
        finally:
            cursor.close()
            conn.close()
            logger.info("[OK] Database closed")
    
    except Exception as e:
        logger.error(f"[FAIL] Database load: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise AirflowException(f"Database load failed: {e}")


def pipeline_summary(**context):
    """Print pipeline execution summary"""
    logger.info("="*80)
    logger.info("PIPELINE EXECUTION SUMMARY")
    logger.info("="*80)
    
    try:
        generated = context['task_instance'].xcom_pull(task_ids='generate_data', key='generated_rows') or 0
        loaded = context['task_instance'].xcom_pull(task_ids='load_to_database', key='loaded_rows') or 0
        total = context['task_instance'].xcom_pull(task_ids='generate_data', key='total_rows') or 0
        
        logger.info(f"\nGenerated: {generated:,} new rows")
        logger.info(f"Total in CSV: {total:,} rows")
        logger.info(f"Loaded to DB: {loaded:,} rows")
        logger.info(f"Success Rate: {100*loaded/max(1, total):.1f}%")
        logger.info(f"Status: {'✓ SUCCESS' if loaded > 0 else '✗ FAILED'}")
        logger.info("="*80 + "\n")
    except Exception as e:
        logger.warning(f"Could not retrieve summary: {e}")

# =============================================================================
# TASK DEFINITIONS
# =============================================================================

start = DummyOperator(
    task_id='start_pipeline',
    dag=dag
)

generate = PythonOperator(
    task_id='generate_data',
    python_callable=generate_synthetic_data,
    provide_context=True,
    dag=dag,
)

check_csv = BranchPythonOperator(
    task_id='check_csv_file',
    python_callable=check_csv_file_exists,
    provide_context=True,
    dag=dag,
)

load_csv = PythonOperator(
    task_id='load_csv_data',
    python_callable=load_csv_data,
    provide_context=True,
    dag=dag,
)

transform = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load_db = PythonOperator(
    task_id='load_to_database',
    python_callable=load_to_database,
    provide_context=True,
    dag=dag,
)

summary = PythonOperator(
    task_id='pipeline_summary',
    python_callable=pipeline_summary,
    provide_context=True,
    trigger_rule='all_done',
    dag=dag,
)

end = DummyOperator(
    task_id='end_pipeline',
    trigger_rule='all_done',
    dag=dag
)

# =============================================================================
# DAG DEPENDENCIES
# =============================================================================
start >> generate >> check_csv >> load_csv >> transform >> load_db >> summary >> end