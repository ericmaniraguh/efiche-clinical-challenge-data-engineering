-- ===================================================================
-- Create Clinical Database if it doesn't exist
-- ===================================================================
DO
$body$
BEGIN
   IF NOT EXISTS (
      SELECT FROM pg_database WHERE datname = 'efiche_clinical_database'
   ) THEN
      PERFORM dblink_exec('dbname=postgres', 'CREATE DATABASE efiche_clinical_database');
   END IF;
END
$body$;

DO
$body$
BEGIN
   IF NOT EXISTS (
      SELECT FROM pg_database WHERE datname = 'efiche_clinical_db_analytics'
   ) THEN
      PERFORM dblink_exec('dbname=postgres', 'CREATE DATABASE efiche_clinical_db_analytics');
   END IF;
END
$body$;

-- ===================================================================
-- Grant privileges to postgres user
-- ===================================================================
GRANT ALL PRIVILEGES ON DATABASE efiche_clinical_database TO postgres;
GRANT ALL PRIVILEGES ON DATABASE efiche_clinical_db_analytics TO postgres;
