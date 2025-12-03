# Connecting Your Analytics Database to Superset

Since the Superset instance is running in Docker and the database is running on the host machine (managed by another Docker container exposed on port 5434), you need to use a specific connection string.

## Step 1: Log in to Superset

1.  Open the browser and go to [http://localhost:8089](http://localhost:8089).
2.  Log in with:
    - **Username:** `superset_admin`
    - **Password:** `admin`

## Step 2: Add a New Database

1.  In the top-right corner, click **Settings** (the gear icon) -> **Database Connections**.
2.  Click the **+ Database** button in the top-right.
3.  Select **PostgreSQL** from the list of supported databases.

## Step 3: Enter Connection Details

Fill in the form with the following details. Note that we use `host.docker.internal` to allow the Superset container to talk to your Windows host.

- **HOST:** `host.docker.internal`
- **PORT:** `5434`
- **DATABASE NAME:** `efiche_clinical_db_analytics`
- **USERNAME:** `postgres`
- **PASSWORD:** `admin`
- **DISPLAY NAME:** `Analytics DB` (or any name you prefer)

### Alternative: SQLAlchemy URI

If you prefer to enter the connection string directly (or if the form asks for a URI), use this:

```
postgresql+psycopg2://postgres:admin@host.docker.internal:5434/efiche_clinical_db_analytics
```

## Step 4: Test and Finish

1.  Click **Test Connection**. You should see a "Connection looks good!" message.
2.  Click **Connect** (or **Finish**).

## Step 5: Create a Dataset

1.  Go to **Datasets** (top menu) -> **+ Dataset**.
2.  Select your new database (`Analytics DB`).
3.  Select the schema (e.g., `analytics`, `public`, or `reporting`).
4.  Select a table (e.g., `fact_clinical_data` or similar).
5.  Click **Add**.

You can now start creating charts and dashboards with your data!
