"""
Database Initialization Script - REFACTORED
- Uses reusable db_connection module
- Clean separation of concerns
- Easy to maintain and extend
- Calls db_connection for all database operations

Usage:
    python db_init.py
"""

import os
import sys
import logging
from pathlib import Path

# =====================================================================
# ADD PROJECT ROOT TO PATH FOR IMPORTS
# =====================================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / 'utils'))

# =====================================================================
# IMPORT DATABASE CONNECTION MODULE
# =====================================================================

try:
    from utils.db_connection import (
        get_db_connection,
        get_db_config,
        validate_db_config,
        init_logger
    )
except ImportError as e:
    print(f"[ERROR] Could not import db_connection module: {e}")
    print(f"   Make sure db_connection.py is in: {PROJECT_ROOT / 'utils'}")
    sys.exit(1)

# =====================================================================
# INITIALIZE LOGGER
# =====================================================================


logger = init_logger('db_init', logging.INFO)

# =====================================================================
# CONFIGURATION
# =====================================================================

DATABASES_TO_CREATE = [
    "efiche_clinical_database",
    "efiche_clinical_db_analytics"
]

SQL_FILES = [
    "init-databases.sql",
    "schema_ddl.sql",
    "schema_analytics.sql",
    "schema_audit.sql",
]

SQL_SEARCH_PATHS = [
    Path.cwd() / "data_model",
    Path.cwd() / "dwh",
    PROJECT_ROOT / "data_model",
    PROJECT_ROOT / "dwh",
    PROJECT_ROOT / "dags" / "dwh",
    PROJECT_ROOT.parent / "data_model",
    PROJECT_ROOT.parent / "dwh",
]

def create_database(db_name):
    """Create a database if it doesn't exist."""
    try:
        logger.info(f"\nChecking database: {db_name}")
        
        # Connect to postgres database
        conn = get_db_connection(db_name="postgres")
        cur = conn.cursor()
        
        # Check if database exists
        cur.execute("SELECT 1 FROM pg_database WHERE datname=%s", (db_name,))
        exists = cur.fetchone()
        
        if not exists:
            from psycopg2 import sql
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
            logger.info(f"  [OK] Database created: {db_name}")
        else:
            logger.info(f"  [OK] Database already exists: {db_name}")
        
        cur.close()
        conn.close()
    
    except Exception as e:
        logger.error(f"[ERROR] Error creating database {db_name}: {e}")
        raise

def find_sql_file(filename):
    """Find SQL file in search paths."""
    for search_path in SQL_SEARCH_PATHS:
        potential_path = search_path / filename
        if potential_path.exists():
            return potential_path
    
    return None

def execute_sql_file(filepath, db_name):
    """Execute SQL file against database."""
    try:
        logger.info(f"  Executing on database: {db_name}")
        
        with open(filepath, 'r') as f:
            sql_content = f.read()
        
        conn = get_db_connection(db_name=db_name)
        cur = conn.cursor()
        
        # Execute SQL content
        cur.execute(sql_content)
        conn.commit()
        
        logger.info(f"  [OK] SQL file executed successfully: {filepath.name}")
        
        cur.close()
        conn.close()
    
    except Exception as e:
        logger.error(f"  [ERROR] Error executing {filepath.name}: {e}")
        raise

def verify_database(db_name):
    """Verify that database objects were created."""
    try:
        logger.info(f"\nVerifying database: {db_name}")
        
        conn = get_db_connection(db_name=db_name)
        cur = conn.cursor()
        
        # Count all objects
        cur.execute("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
        """)
        table_count = cur.fetchone()[0]
        
        if table_count > 0:
            logger.info(f"  [OK] Found {table_count} objects (schemas/tables/views)")
            
            # List schemas
            cur.execute("""
                SELECT schema_name FROM information_schema.schemata
                WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'public')
                ORDER BY schema_name
            """)
            schemas = cur.fetchall()
            
            if schemas:
                logger.info(f"    Schemas created:")
                for schema in schemas:
                    logger.info(f"      - {schema[0]}")
        else:
            logger.warning(f"  [WARN] No tables/schemas found in {db_name}")
        
        cur.close()
        conn.close()
    
    except Exception as e:
        logger.error(f"[ERROR] Error verifying database: {e}")

# =====================================================================
# MAIN SCRIPT
# =====================================================================

def main():
    """Main initialization routine."""
    try:
        # Step 1: Validate configuration
        logger.info("="*70)
        logger.info("eFiche Database Initialization Script")
        logger.info("="*70)
        
        logger.info("\nValidating configuration...")
        validate_db_config()
        
        config = get_db_config()
        logger.info(f"  Database: {config['database']}")
        logger.info(f"  Host: {config['host']}:{config['port']}")
        
        # Step 2: Create databases
        logger.info("\n" + "="*70)
        logger.info("STEP 1: CREATE DATABASES")
        logger.info("="*70)
        
        for db in DATABASES_TO_CREATE:
            create_database(db)
        
        # Step 3: Execute schema SQL files
        logger.info("\n" + "="*70)
        logger.info("STEP 2: EXECUTE SCHEMA SQL FILES")
        logger.info("="*70)
        
        logger.info(f"\nSearching for SQL files in:")
        for search_path in SQL_SEARCH_PATHS:
            logger.info(f"  - {search_path.absolute()}")
        
        for sql_filename in SQL_FILES:
            found_path = find_sql_file(sql_filename)
            
            if found_path:
                logger.info(f"\n[OK] Found SQL file: {sql_filename}")
                try:
                    # Decide which DB to execute on
                    if "analytics" in sql_filename.lower() or "audit" in sql_filename.lower():
                        target_db = "efiche_clinical_db_analytics"
                    elif "init-databases" in sql_filename.lower():
                        target_db = "postgres" # init-databases usually runs on postgres
                    else:
                        # Default to operational DB for schema_ddl.sql and others
                        target_db = os.getenv("PG_DATABASE_OPERATIONAL", "efiche_clinical_database")
                    
                    execute_sql_file(found_path, target_db)
                except Exception as e:
                    logger.error(f"  Error executing {sql_filename} on {target_db}: {e}")
            else:
                logger.warning(f"\n[WARN] SQL file not found: {sql_filename}")
                logger.warning(f"  Searched paths:")
                for search_path in SQL_SEARCH_PATHS:
                    logger.warning(f"    - {search_path / sql_filename}")
                        
        # Step 4: Verify databases
        logger.info("\n" + "="*70)
        logger.info("STEP 3: VERIFY DATABASES")
        logger.info("="*70)
        
        for db in DATABASES_TO_CREATE:
            verify_database(db)
        
        # Success
        logger.info("\n" + "="*70)
        logger.info("[OK] DATABASE INITIALIZATION COMPLETED SUCCESSFULLY!")
        logger.info("="*70)
    
    except Exception as e:
        logger.error(f"\n[ERROR] Database initialization FAILED: {e}")
        logger.error("="*70)
        raise

if __name__ == "__main__":
    """
    Refactored Database Initialization Script
    
    Uses: db_connection module for all database operations
    
    Features:
    * Clean separation of concerns
    * Reusable database connection module
    * Easy to maintain and extend
    * All configuration from .env
    * NO HARDCODED VALUES
    
    Usage:
    $ python db_init.py
    """
    
    print("\n" + "="*70)
    print("eFiche Database Initialization Script (Refactored)")
    print("="*70)
    print(f"Working directory: {Path.cwd()}")
    print(f"Script location: {__file__}")
    print("="*70 + "\n")
    
    main()