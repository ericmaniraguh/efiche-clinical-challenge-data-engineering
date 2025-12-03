import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5434')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'admin')

def list_dbs():
    try:
        # List Databases
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database='postgres'
        )
        cur = conn.cursor()
        cur.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")
        rows = cur.fetchall()
        print(f"Databases on {DB_HOST}:{DB_PORT}:")
        for row in rows:
            print(f" - {row[0]}")
        conn.close()

        # List Schemas in efiche_clinical_database
        print(f"\nSchemas in efiche_clinical_database:")
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database='efiche_clinical_database'
        )
        cur = conn.cursor()
        cur.execute("SELECT schema_name FROM information_schema.schemata;")
        rows = cur.fetchall()
        for row in rows:
            print(f" - {row[0]}")
        conn.close()

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    list_dbs()
