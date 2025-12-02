import os
import sys
from pathlib import Path
from dotenv import load_dotenv
import psycopg2

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Load env
load_dotenv(PROJECT_ROOT / ".env")

def get_db_connection():
    host = os.getenv("EFICHE_DB_HOST_INTERNAL", "localhost")
    port = int(os.getenv("EFICHE_DB_PORT_INTERNAL", 5433))
    user = os.getenv("PG_USER", "postgres")
    password = os.getenv("PG_PASSWORD", "admin")
    database = os.getenv("PG_DATABASE_OPERATIONAL", "efiche_clinical_database")
    
    print(f"Connecting to {database} at {host}:{port}...")
    return psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database
    )

def verify_data():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        tables = [
            "operational.patients",
            "operational.encounters",
            "operational.procedures",
            "operational.radiological_images",
            "operational.clinical_reports",
            "operational.findings",
            "operational.procedure_diagnosis",
            "master.facility_master",
            "master.modality_master",
            "master.diagnosis_master"
        ]
        
        print("\n=== Row Counts ===")
        for table in tables:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            print(f"{table}: {count}")
            
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    verify_data()
