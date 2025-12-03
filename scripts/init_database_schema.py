#!/usr/bin/env python3
"""
Initialize eFiche database schema
Runs SQL files in correct order to create all tables and relationships
Handles both Operational and Analytics databases
"""

import os
import sys
import logging
import psycopg2
from pathlib import Path
from dotenv import load_dotenv

# Load environment
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database config
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = int(os.getenv('DB_PORT', '5434')) # Default to 5434 as seen in docker ps
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'admin')
DB_NAME_OPS = os.getenv('DB_NAME', 'efiche_clinical_database')
DB_NAME_ANALYTICS = os.getenv('DB_ANALYTICS_NAME', 'efiche_clinical_db_analytics')

def create_database_if_not_exists(db_name):
    """Create database if it doesn't exist"""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database='postgres',
            connect_timeout=10
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'")
        exists = cursor.fetchone()
        
        if not exists:
            logger.info(f"Creating database: {db_name}")
            cursor.execute(f"CREATE DATABASE {db_name}")
            logger.info(f"✓ Database created: {db_name}")
        else:
            logger.info(f"✓ Database exists: {db_name}")
        
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Error creating database {db_name}: {e}")
        return False

def init_db(db_name, sql_files):
    """Initialize a specific database with SQL files"""
    logger.info(f"\nInitializing {db_name}...")
    
    if not create_database_if_not_exists(db_name):
        return False

    # Create schemas
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=db_name
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Create all potential schemas to be safe
        schemas = ['master', 'operational', 'analytics', 'reporting', 'audit', 'staging']
        for schema in schemas:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"Error creating schemas in {db_name}: {e}")
        return False

    # Execute SQL files
    project_root = Path(__file__).parent.parent
    all_success = True
    
    for sql_file in sql_files:
        filepath = project_root / sql_file
        logger.info(f"Executing {sql_file}...")
        
        if not os.path.exists(filepath):
            logger.error(f"SQL file not found: {filepath}")
            all_success = False
            continue

        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                sql_script = f.read()
            
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                user=DB_USER,
                password=DB_PASSWORD,
                database=db_name
            )
            conn.autocommit = True
            cursor = conn.cursor()
            cursor.execute(sql_script)
            cursor.close()
            conn.close()
            logger.info(f"✓ Executed: {sql_file}")
        except Exception as e:
            logger.error(f"✗ Error executing {sql_file}: {e}")
            all_success = False

    return all_success

def main():
    logger.info("="*80)
    logger.info("INITIALIZING EFICHE DATABASE SCHEMAS")
    logger.info("="*80)
    
    # 1. Initialize Operational DB
    ops_files = [
        'data_model/schema_ddl.sql',
        'dags/dwh/schema_audit.sql' # Audit schema is needed in operational too for triggers
    ]
    if not init_db(DB_NAME_OPS, ops_files):
        return False
        
    # 2. Initialize Analytics DB
    analytics_files = [
        'dags/dwh/schema_analytics.sql',
        'dags/dwh/schema_audit.sql' # Audit schema is needed in analytics too
    ]
    if not init_db(DB_NAME_ANALYTICS, analytics_files):
        return False
        
    logger.info("\n" + "="*80)
    logger.info("✓ ALL DATABASES INITIALIZED SUCCESSFULLY")
    logger.info("="*80)
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)