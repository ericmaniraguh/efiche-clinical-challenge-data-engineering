#!/usr/bin/env python3
"""
Initialize eFiche database schema
Runs SQL files in correct order to create all tables and relationships
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
DB_PORT = int(os.getenv('DB_PORT', '5433'))
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'admin')
DB_NAME = os.getenv('DB_NAME', 'efiche_clinical_database')

# SQL files in correct execution order
SQL_FILES = [
    'data_model/init-databases.sql',      # Creates database
    'data_model/schema_ddl.sql',          # Creates schemas & tables
    'dwh/schema_analytics.sql',           # Creates analytics schema
]

def execute_sql_file(filepath, conn=None):
    """Execute SQL file against database"""
    if not os.path.exists(filepath):
        logger.error(f"SQL file not found: {filepath}")
        return False
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            sql_script = f.read()
        
        if conn is None:
            # Connect to database
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                user=DB_USER,
                password=DB_PASSWORD,
                database=DB_NAME,
                connect_timeout=10
            )
            close_conn = True
        else:
            close_conn = False
        
        # Use psycopg2's autocommit mode for executing multiple statements
        # This is necessary because cursor.execute() in default mode only
        # executes the first statement in a multi-statement script
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Execute the entire script (PostgreSQL will parse and execute all statements)
        cursor.execute(sql_script)
        cursor.close()
        
        if close_conn:
            conn.close()
        
        logger.info(f"✓ Executed: {filepath}")
        return True
    
    except Exception as e:
        logger.error(f"✗ Error executing {filepath}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        if conn:
            try:
                conn.rollback()
            except:
                pass
        return False

def main():
    """Initialize database schema"""
    logger.info("="*80)
    logger.info("INITIALIZING EFICHE DATABASE SCHEMA")
    logger.info("="*80)
    
    # Get project root
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    
    try:
        # First, check if database exists; if not, create it
        logger.info("\n[STEP 1] Connecting to PostgreSQL...")
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                user=DB_USER,
                password=DB_PASSWORD,
                database='postgres',  # Connect to default postgres database
                connect_timeout=10
            )
            cursor = conn.cursor()
            
            # Check if database exists
            cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{DB_NAME}'")
            exists = cursor.fetchone()
            
            if not exists:
                logger.info(f"Creating database: {DB_NAME}")
                conn.autocommit = True
                cursor.execute(f"CREATE DATABASE {DB_NAME}")
                logger.info(f"✓ Database created: {DB_NAME}")
            else:
                logger.info(f"✓ Database exists: {DB_NAME}")
            
            cursor.close()
            conn.close()
        except Exception as e:
            logger.error(f"Could not check/create database: {e}")
            return False
        
        # Now execute SQL files against the target database
        logger.info(f"\n[STEP 2] Executing schema files...")
        
        all_success = True
        for sql_file in SQL_FILES:
            filepath = project_root / sql_file
            if not execute_sql_file(str(filepath)):
                all_success = False
        
        if all_success:
            logger.info("\n" + "="*80)
            logger.info("✓ DATABASE INITIALIZATION COMPLETE")
            logger.info("="*80)
            return True
        else:
            logger.error("\n✗ Some schema files failed to execute")
            return False
    
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
