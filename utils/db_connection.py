"""
Database Connection Module - UPDATED FOR TWO-DATABASE SETUP
- Uses DB_NAME for operational database (source data)
- Uses DB_ANALYTICS_NAME for analytics database (warehouse)
- Handles both connections properly

Usage:
    from utils.db_connection import get_db_connection, get_db_config
    
    # For analytics operations (default)
    conn = get_db_connection()
    
    # For operational database (if needed)
    conn = get_db_connection(db_type='operational')
"""

import os
import sys
import logging
from pathlib import Path
from dotenv import load_dotenv
import psycopg2
from psycopg2 import pool, Error

# ============================================================================
# LOAD ENVIRONMENT VARIABLES
# ============================================================================

# Find and load .env file from project root
PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = PROJECT_ROOT / '.env'

if ENV_FILE.exists():
    load_dotenv(ENV_FILE)
else:
    # Try parent directory
    ENV_FILE = PROJECT_ROOT.parent / '.env'
    if ENV_FILE.exists():
        load_dotenv(ENV_FILE)

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================

def init_logger(name, level=logging.INFO):
    """Initialize and return a logger"""
    logger = logging.getLogger(name)
    
    if not logger.handlers:
        formatter = logging.Formatter(
            '[%(levelname)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    logger.setLevel(level)
    return logger

logger = init_logger('db_connection', logging.INFO)

# ============================================================================
# DATABASE CONFIGURATION
# ============================================================================

def get_db_config(db_type='analytics'):
    """
    Get database configuration from environment variables
    
    Args:
        db_type: 'analytics' (default) or 'operational'
    
    Returns:
        Dictionary with database configuration
    """
    if db_type == 'operational':
        database_name = os.getenv('DB_NAME', 'efiche_clinical_database')
    else:  # analytics (default)
        database_name = os.getenv('DB_ANALYTICS_NAME', 'efiche_clinical_db_analytics')
    
    return {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', 5432)),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', 'postgres'),
        'database': database_name,
    }

def validate_db_config():
    """Validate that database configuration is properly set"""
    required_vars = ['DB_HOST', 'DB_PORT', 'DB_USER', 'DB_PASSWORD', 'DB_NAME', 'DB_ANALYTICS_NAME']
    
    logger.info("Validating database configuration...")
    
    for var in required_vars:
        value = os.getenv(var)
        if not value:
            logger.error(f"  Missing environment variable: {var}")
            raise ValueError(f"Missing required environment variable: {var}")
        
        # Show masked password
        if var == 'DB_PASSWORD':
            logger.info(f"  ✓ {var} = {'*' * len(value)}")
        else:
            logger.info(f"  ✓ {var} = {value}")
    
    logger.info("✓ Database configuration validated")

# ============================================================================
# CONNECTION MANAGEMENT
# ============================================================================

# Connection pool for connection reuse
_connection_pools = {}

def get_db_connection(db_type='analytics', db_name=None):
    """
    Get a database connection from pool or create new one
    
    Args:
        db_type: 'analytics' (default) or 'operational'
        db_name: Override database name (optional)
    
    Returns:
        psycopg2 connection object
    """
    try:
        config = get_db_config(db_type=db_type)
        
        if db_name:
            config['database'] = db_name
        
        # Create connection string
        conn_string = f"postgresql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"
        
        # Connect
        conn = psycopg2.connect(
            host=config['host'],
            port=config['port'],
            user=config['user'],
            password=config['password'],
            database=config['database'],
            connect_timeout=10
        )
        
        logger.info(f"✓ Connected to {config['host']}:{config['port']}/{config['database']}")
        
        return conn
    
    except psycopg2.Error as e:
        logger.error(f"Database connection failed: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error connecting to database: {e}")
        raise

def close_connection(conn):
    """Close a database connection"""
    try:
        if conn and not conn.closed:
            conn.close()
            logger.info("Database connection closed")
    except Exception as e:
        logger.error(f"Error closing connection: {e}")

# ============================================================================
# QUERY EXECUTION
# ============================================================================

def execute_query(query, conn=None, fetch_all=False, db_type='analytics'):
    """
    Execute a SQL query
    
    Args:
        query: SQL query string
        conn: Database connection (optional - will create if not provided)
        fetch_all: Return all results (True) or just first (False)
        db_type: 'analytics' or 'operational'
    
    Returns:
        Query results or None
    """
    close_conn = False
    
    try:
        if conn is None:
            conn = get_db_connection(db_type=db_type)
            close_conn = True
        
        cur = conn.cursor()
        cur.execute(query)
        
        if fetch_all:
            results = cur.fetchall()
        else:
            results = cur.fetchone()
        
        cur.close()
        
        return results
    
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        raise
    
    finally:
        if close_conn:
            close_connection(conn)

def execute_batch(queries, db_type='analytics', commit=True):
    """
    Execute multiple SQL queries in sequence
    
    Args:
        queries: List of SQL query strings
        db_type: 'analytics' or 'operational'
        commit: Commit after execution (default True)
    
    Returns:
        List of results
    """
    conn = None
    results = []
    
    try:
        conn = get_db_connection(db_type=db_type)
        cur = conn.cursor()
        
        for query in queries:
            cur.execute(query)
            try:
                results.append(cur.fetchall())
            except:
                results.append(None)
        
        if commit:
            conn.commit()
            logger.info(f"Committed {len(queries)} queries")
        
        cur.close()
        
        return results
    
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Error executing batch: {e}")
        raise
    
    finally:
        close_connection(conn)

# ============================================================================
# MAIN - TEST CONNECTION
# ============================================================================

if __name__ == "__main__":
    print("\n" + "="*70)
    print("Database Connection Module - Testing")
    print("="*70 + "\n")
    
    try:
        # Validate config
        validate_db_config()
        
        # Get both configs
        print("\nDatabase Configurations:")
        print("-" * 70)
        
        analytics_config = get_db_config('analytics')
        print(f"Analytics DB:")
        print(f"  Host: {analytics_config['host']}:{analytics_config['port']}")
        print(f"  Database: {analytics_config['database']}")
        print(f"  User: {analytics_config['user']}")
        
        operational_config = get_db_config('operational')
        print(f"\nOperational DB:")
        print(f"  Host: {operational_config['host']}:{operational_config['port']}")
        print(f"  Database: {operational_config['database']}")
        print(f"  User: {operational_config['user']}")
        
        # Test analytics connection
        print("\n" + "-" * 70)
        print("Testing Analytics Database Connection...")
        conn_analytics = get_db_connection('analytics')
        if conn_analytics:
            print("✓ Analytics database connection successful")
            close_connection(conn_analytics)
        
        # Test operational connection
        print("\nTesting Operational Database Connection...")
        conn_operational = get_db_connection('operational')
        if conn_operational:
            print("✓ Operational database connection successful")
            close_connection(conn_operational)
        
        print("\n" + "="*70)
        print("✓ All tests passed!")
        print("="*70 + "\n")
    
    except Exception as e:
        print(f"\n✗ Error: {e}\n")
        sys.exit(1)