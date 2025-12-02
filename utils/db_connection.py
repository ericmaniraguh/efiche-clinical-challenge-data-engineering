"""
Database Connection Module - FINAL VERSION (MATCHES YOUR .env)
- Operational DB   = PG_DATABASE_OPERATIONAL
- Analytics DB     = PG_DATABASE_ANALYTICS
- Host/Port        = EFICHE_DB_HOST_INTERNAL / EFICHE_DB_PORT_INTERNAL
"""

import os
import sys
import logging
from pathlib import Path
from dotenv import load_dotenv
import psycopg2

# ============================================================================
# LOAD ENVIRONMENT VARIABLES
# ============================================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = PROJECT_ROOT / ".env"

if ENV_FILE.exists():
    load_dotenv(ENV_FILE)
else:
    load_dotenv(PROJECT_ROOT.parent / ".env")


# ============================================================================
# LOGGING
# ============================================================================

def init_logger(name, level=logging.INFO):
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("[%(levelname)s] %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(level)
    return logger

logger = init_logger("db_connection")


# ============================================================================
# CONFIGURATION
# ============================================================================

def get_db_config(db_type="analytics"):
    """
    Return PostgreSQL connection config based on .env
    """
    host = os.getenv("EFICHE_DB_HOST_INTERNAL", os.getenv("DB_HOST", "host.docker.internal"))
    port = int(os.getenv("EFICHE_DB_PORT_INTERNAL", os.getenv("DB_PORT", 5434)))
    user = os.getenv("PG_USER", "postgres")
    password = os.getenv("PG_PASSWORD", "admin")

    if db_type == "operational":
        database = os.getenv("PG_DATABASE_OPERATIONAL", "efiche_clinical_database")
    else:
        database = os.getenv("PG_DATABASE_ANALYTICS", "efiche_clinical_db_analytics")

    return {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "database": database,
    }


def validate_db_config():
    """Ensure .env contains required variables"""
    required = [
        "PG_USER",
        "PG_PASSWORD",
        "PG_DATABASE_OPERATIONAL",
        "PG_DATABASE_ANALYTICS",
        "EFICHE_DB_HOST_INTERNAL",
        "EFICHE_DB_PORT_INTERNAL",
    ]

    logger.info("Validating .env database variables...")

    for var in required:
        val = os.getenv(var)
        if not val:
            logger.error(f"❌ Missing env var: {var}")
            raise ValueError(f"Missing environment variable: {var}")
        if var == "PG_PASSWORD":
            logger.info(f"✓ {var} = {'*' * len(val)}")
        else:
            logger.info(f"✓ {var} = {val}")


# ============================================================================
# CONNECTION HANDLING
# ============================================================================

def get_db_connection(db_type="analytics", db_name=None):
    """
    Connect to PostgreSQL using pooled or direct connection.
    """
    try:
        cfg = get_db_config(db_type)
        
        # Override database name if provided
        dbname = db_name if db_name else cfg["database"]

        conn = psycopg2.connect(
            host=cfg["host"],
            port=cfg["port"],
            user=cfg["user"],
            password=cfg["password"],
            database=dbname,
            connect_timeout=10,
        )

        logger.info(f"✓ Connected to {cfg['host']}:{cfg['port']} DB={dbname}")
        return conn

    except Exception as e:
        # Fallback for local execution if host.docker.internal fails
        if cfg["host"] == "host.docker.internal":
            logger.warning(f"Connection to host.docker.internal failed: {e}")
            logger.warning("Attempting fallback to localhost...")
            try:
                conn = psycopg2.connect(
                    host="localhost",
                    port=cfg["port"],
                    user=cfg["user"],
                    password=cfg["password"],
                    database=dbname,
                    connect_timeout=10,
                )
                logger.info(f"✓ Connected to localhost:{cfg['port']} DB={dbname} (Fallback)")
                return conn
            except Exception as e2:
                logger.error(f"Fallback to localhost failed: {e2}")
                raise e # Raise original error if fallback fails
        
        logger.error(f"Database connection failed: {str(e)}")
        raise


def close_connection(conn):
    try:
        if conn and not conn.closed:
            conn.close()
            logger.info("✓ Connection closed")
    except Exception as e:
        logger.error(f"Error closing connection: {e}")


# ============================================================================
# QUERY HELPERS
# ============================================================================

def execute_query(query, conn=None, fetch_all=False, db_type="analytics"):
    close_conn = False
    try:
        if conn is None:
            conn = get_db_connection(db_type)
            close_conn = True

        cur = conn.cursor()
        cur.execute(query)

        result = cur.fetchall() if fetch_all else cur.fetchone()
        cur.close()

        return result

    except Exception as e:
        logger.error(f"Query failed: {e}")
        raise

    finally:
        if close_conn:
            close_connection(conn)


def execute_batch(queries, db_type="analytics", commit=True):
    conn = None
    results = []

    try:
        conn = get_db_connection(db_type)
        cur = conn.cursor()

        for q in queries:
            cur.execute(q)
            try:
                results.append(cur.fetchall())
            except:
                results.append(None)

        if commit:
            conn.commit()
            logger.info(f"✓ Batch committed ({len(queries)} queries)")

        cur.close()
        return results

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Batch execution error: {e}")
        raise

    finally:
        close_connection(conn)


# ============================================================================
# SELF TEST
# ============================================================================

if __name__ == "__main__":
    print("\n===== DB Connection Test =====\n")
    try:
        validate_db_config()

        print("\nTesting analytics DB...")
        conn = get_db_connection("analytics")
        close_connection(conn)

        print("\nTesting operational DB...")
        conn = get_db_connection("operational")
        close_connection(conn)

        print("\n✓ ALL TESTS PASSED\n")

    except Exception as e:
        print(f"❌ ERROR: {e}")
        sys.exit(1)
