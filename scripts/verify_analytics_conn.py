import psycopg2
import sys

DB_HOST = "host.docker.internal"
DB_PORT = "5434"
DB_USER = "postgres"
DB_PASSWORD = "admin"
DB_NAME = "efiche_clinical_db_analytics"

try:
    print(f"Attempting to connect to {DB_NAME} at {DB_HOST}:{DB_PORT}...")
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )
    print("Successfully connected!")
    cur = conn.cursor()
    cur.execute("SELECT schema_name FROM information_schema.schemata;")
    rows = cur.fetchall()
    print("Available schemas:")
    for row in rows:
        print(f" - {row[0]}")
    conn.close()
except Exception as e:
    print(f"Connection failed: {e}")
    sys.exit(1)
