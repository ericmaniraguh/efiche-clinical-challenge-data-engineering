"""
eFiche Airflow Configuration
Centralizes configuration for the eFiche ETL pipeline
"""

import os
from datetime import timedelta
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# ============================================================================
# AIRFLOW CORE CONFIGURATION
# ============================================================================

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
AIRFLOW_UID = int(os.getenv("AIRFLOW_UID", 50000))
AIRFLOW_GID = int(os.getenv("AIRFLOW_GID", 50000))

# ============================================================================
# DATABASE CONFIGURATION
# ============================================================================

# Airflow Metadata Database (PostgreSQL)
AIRFLOW_DB_USER = os.getenv("POSTGRES_USER", "airflow")
AIRFLOW_DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "airflow")
AIRFLOW_DB_HOST = os.getenv("AIRFLOW_DB_HOST", "airflow_postgres")
AIRFLOW_DB_PORT = int(os.getenv("AIRFLOW_DB_PORT", 5432))
AIRFLOW_DB_NAME = os.getenv("AIRFLOW_DB_NAME", "airflow_db")

# Airflow SQL Alchemy Connection String
AIRFLOW_SQL_ALCHEMY_CONN = (
    f"postgresql+psycopg2://{AIRFLOW_DB_USER}:{AIRFLOW_DB_PASSWORD}"
    f"@{AIRFLOW_DB_HOST}:{AIRFLOW_DB_PORT}/{AIRFLOW_DB_NAME}"
)

# eFiche Clinical Database (for ETL operations)
EFICHE_DB_USER = os.getenv("DB_USER", "postgres")
EFICHE_DB_PASSWORD = os.getenv("DB_PASSWORD", "admin")
EFICHE_DB_HOST = os.getenv("DB_HOST", "localhost")
EFICHE_DB_PORT = int(os.getenv("DB_PORT", 5433))
EFICHE_DB_NAME = os.getenv("DB_NAME", "efiche_clinical_database")

EFICHE_SQL_ALCHEMY_CONN = (
    f"postgresql+psycopg2://{EFICHE_DB_USER}:{EFICHE_DB_PASSWORD}"
    f"@{EFICHE_DB_HOST}:{EFICHE_DB_PORT}/{EFICHE_DB_NAME}"
)

# ============================================================================
# AIRFLOW EXECUTOR CONFIGURATION
# ============================================================================

AIRFLOW_EXECUTOR = os.getenv("AIRFLOW__CORE__EXECUTOR", "LocalExecutor")

# For LocalExecutor (single machine, sequential or parallel tasks)
AIRFLOW_PARALLELISM = int(os.getenv("AIRFLOW_PARALLELISM", 32))
AIRFLOW_MAX_ACTIVE_RUNS_PER_DAG = int(os.getenv("AIRFLOW_MAX_ACTIVE_RUNS_PER_DAG", 1))

# For CeleryExecutor (distributed, requires Redis/RabbitMQ broker)
AIRFLOW_BROKER_URL = os.getenv("AIRFLOW_BROKER_URL", "redis://efiche_redis:6379/0")
AIRFLOW_RESULT_BACKEND = os.getenv("AIRFLOW_RESULT_BACKEND", "db+postgresql://airflow:airflow@airflow_postgres:5432/airflow_db")

# ============================================================================
# DAG CONFIGURATION
# ============================================================================

DAG_OWNER = os.getenv("DAG_OWNER", "efiche_data_team")
DAG_EMAIL = os.getenv("DAG_EMAIL", "admin@efiche.local")
DAG_EMAIL_ON_FAILURE = os.getenv("DAG_EMAIL_ON_FAILURE", "false").lower() == "true"
DAG_EMAIL_ON_RETRY = os.getenv("DAG_EMAIL_ON_RETRY", "false").lower() == "true"

# Default DAG arguments
DEFAULT_DAG_ARGS = {
    "owner": DAG_OWNER,
    "depends_on_past": False,
    "start_date": None,  # Will be set in DAG definition
    "email": [DAG_EMAIL],
    "email_on_failure": DAG_EMAIL_ON_FAILURE,
    "email_on_retry": DAG_EMAIL_ON_RETRY,
    "retries": int(os.getenv("TASK_RETRIES", 2)),
    "retry_delay": timedelta(minutes=int(os.getenv("TASK_RETRY_DELAY_MINUTES", 5))),
    "execution_timeout": timedelta(hours=int(os.getenv("TASK_EXECUTION_TIMEOUT_HOURS", 2))),
    "pool": "default_pool",
    "pool_slots": 1,
}

# ============================================================================
# ETL CONFIGURATION
# ============================================================================

# Data Generation
NUM_ROWS = int(os.getenv("NUM_ROWS", 1000))
CSV_FILE = os.getenv("ETL_CSV_FILE", "/opt/airflow/data/padchest_synthetic_data.csv")
STATE_FILE = os.getenv("STATE_FILE", "/opt/airflow/data/data_state.json")
APPEND_MODE = os.getenv("APPEND_MODE", "true").lower() == "true"

# ETL Processing
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 1000))
MAX_ROWS = int(os.getenv("MAX_ROWS", 0))  # 0 = no limit
MIN_SUCCESS_RATE = float(os.getenv("MIN_SUCCESS_RATE", 95.0))

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_DIR = os.getenv("LOG_DIR", "/opt/airflow/logs")
LOG_FILE = os.getenv("LOG_FILE", "etl_pipeline.log")
LOG_MAX_BYTES = int(os.getenv("LOG_MAX_BYTES", 10485760))  # 10MB
LOG_BACKUP_COUNT = int(os.getenv("LOG_BACKUP_COUNT", 5))

# ============================================================================
# SECURITY & AUTHENTICATION
# ============================================================================

AIRFLOW_USERNAME = os.getenv("AIRFLOW_USERNAME", "admin")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "admin")
AIRFLOW_EMAIL = os.getenv("AIRFLOW_EMAIL", "admin@efiche.local")
AIRFLOW_FIRSTNAME = os.getenv("AIRFLOW_FIRSTNAME", "Eric")
AIRFLOW_LASTNAME = os.getenv("AIRFLOW_LASTNAME", "Maniraguha")

# API Authentication
AIRFLOW_API_AUTH_BACKENDS = os.getenv(
    "AIRFLOW__API__AUTH_BACKENDS",
    "airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session"
)

# ============================================================================
# WEBSERVER CONFIGURATION
# ============================================================================

AIRFLOW_WEBSERVER_PORT = int(os.getenv("AIRFLOW_WEBSERVER_PORT", 8080))
AIRFLOW_WEBSERVER_WORKERS = int(os.getenv("AIRFLOW_WEBSERVER_WORKERS", 4))
AIRFLOW_WEBSERVER_EXPOSE_CONFIG = os.getenv("AIRFLOW_WEBSERVER_EXPOSE_CONFIG", "true").lower() == "true"
AIRFLOW_WEBSERVER_DAG_DEFAULT_VIEW = os.getenv("AIRFLOW_WEBSERVER_DAG_DEFAULT_VIEW", "graph")

# ============================================================================
# ENVIRONMENT & DEBUG
# ============================================================================

ENVIRONMENT = os.getenv("ENVIRONMENT", "production")
DEBUG = os.getenv("DEBUG", "false").lower() == "true"

# ============================================================================
# FUNCTION: Get DAG arguments dictionary
# ============================================================================

def get_dag_args(start_date=None, dag_owner=None):
    """
    Get default DAG arguments with optional overrides
    
    Args:
        start_date: datetime object for DAG start date
        dag_owner: owner name (defaults to DAG_OWNER from env)
    
    Returns:
        dict: Dictionary of default DAG arguments
    """
    args = DEFAULT_DAG_ARGS.copy()
    if start_date:
        args["start_date"] = start_date
    if dag_owner:
        args["owner"] = dag_owner
    return args


# ============================================================================
# FUNCTION: Get database connection string
# ============================================================================

def get_efiche_db_uri():
    """Get eFiche database connection URI"""
    return EFICHE_SQL_ALCHEMY_CONN


def get_airflow_db_uri():
    """Get Airflow metadata database connection URI"""
    return AIRFLOW_SQL_ALCHEMY_CONN


# ============================================================================
# SUMMARY
# ============================================================================

if __name__ == "__main__":
    print("="*70)
    print("EFICHE AIRFLOW CONFIGURATION SUMMARY")
    print("="*70)
    print(f"\nAirflow Setup:")
    print(f"  Home: {AIRFLOW_HOME}")
    print(f"  Executor: {AIRFLOW_EXECUTOR}")
    print(f"  Parallelism: {AIRFLOW_PARALLELISM}")
    print(f"  Webserver: http://localhost:{AIRFLOW_WEBSERVER_PORT}")
    
    print(f"\nAirflow Metadata Database:")
    print(f"  Host: {AIRFLOW_DB_HOST}:{AIRFLOW_DB_PORT}")
    print(f"  Database: {AIRFLOW_DB_NAME}")
    print(f"  User: {AIRFLOW_DB_USER}")
    
    print(f"\neFiche Clinical Database:")
    print(f"  Host: {EFICHE_DB_HOST}:{EFICHE_DB_PORT}")
    print(f"  Database: {EFICHE_DB_NAME}")
    print(f"  User: {EFICHE_DB_USER}")
    
    print(f"\nETL Configuration:")
    print(f"  CSV File: {CSV_FILE}")
    print(f"  Batch Size: {BATCH_SIZE}")
    print(f"  Append Mode: {APPEND_MODE}")
    
    print(f"\nSecurity:")
    print(f"  Admin User: {AIRFLOW_USERNAME}")
    print(f"  Email: {AIRFLOW_EMAIL}")
    
    print(f"\nLogging:")
    print(f"  Level: {LOG_LEVEL}")
    print(f"  Directory: {LOG_DIR}")
    
    print("="*70)
