# How to Test Queries in Superset (SQL Lab)

To verify that your database connection and query execution are working correctly, follow these steps:

## Step 1: Open SQL Lab

1.  Log in to Superset.
2.  In the top navigation menu, hover over **SQL Lab** and click **SQL Editor**.

## Step 2: Select Your Database

1.  In the left sidebar, look for the **Database** dropdown.
2.  Select your analytics database (e.g., `Analytics DB` or `PostgreSQL`).
3.  **Schema:** Select `analytics`.
4.  **Table Schema:** Select `dim_patient` (optional, just to see columns).

## Step 3: Run a Test Query

Type the following SQL query into the main editor window:

```sql
SELECT *
FROM analytics.dim_patient
LIMIT 10;
```

## Step 4: Execute

1.  Click the **Run** button (or press `Ctrl + Enter`).
2.  **Success:** You should see a table of results appear at the bottom of the screen.
    - This confirms that Superset can talk to your database AND that the query execution engine is working.

## Step 5: Try an Aggregation (Optional)

To test if calculations are working, try this query:

```sql
SELECT
    sex,
    COUNT(*) as patient_count
FROM analytics.dim_patient
GROUP BY sex;
```

If you see results (e.g., Male: 50, Female: 50), everything is working perfectly!
