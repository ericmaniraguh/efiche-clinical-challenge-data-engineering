import psycopg2
import os
from dotenv import load_dotenv
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

load_dotenv()

DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5434')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'admin')

def reset_superset_db():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database='postgres'
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        
        # Terminate connections
        print("Terminating connections to 'superset'...")
        cur.execute("""
            SELECT pg_terminate_backend(pg_stat_activity.pid)
            FROM pg_stat_activity
            WHERE pg_stat_activity.datname = 'superset'
            AND pid <> pg_backend_pid();
        """)
        
        # Drop database if exists
        print("Dropping database 'superset' if it exists...")
        cur.execute("DROP DATABASE IF EXISTS superset;")
        print("Database 'superset' dropped.")

        # Create database
        print("Creating database 'superset'...")
        cur.execute("CREATE DATABASE superset;")
        print("Database 'superset' created successfully.")
            
        conn.close()

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    reset_superset_db()
