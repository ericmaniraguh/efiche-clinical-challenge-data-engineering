# Visualizing Data in Superset: `analytics.dim_patient`

This guide shows you how to create your first chart using the `dim_patient` table.

## Step 1: Create a Dataset

1.  In Superset, click on **Datasets** in the top navigation bar.
2.  Click the **+ Dataset** button (top right).
3.  **Database:** Select your analytics database (e.g., `Analytics DB` or `PostgreSQL`).
4.  **Schema:** Select `analytics`.
5.  **Table:** Select `dim_patient`.
6.  Click **Add**.

## Step 2: Create a Chart

1.  You should now see `dim_patient` in the list of datasets. Click on its name to start creating a chart.
    - _Alternatively, go to **Charts** -> **+ Chart**, select the `dim_patient` dataset, choose a visualization type (e.g., **Pie Chart**), and click **Create New Chart**._

## Step 3: Configure Your Chart (Example: Patients by Gender)

Let's create a simple Pie Chart showing the distribution of patients by gender (`sex`).

1.  **Visualization Type:** Ensure **Pie Chart** is selected (you can change this in the top left).
2.  **Query Section (Left Panel):**
    - **Group by:** Click and select `sex`.
    - **Metric:** Click and select `COUNT(*)` (or simply `COUNT`).
3.  **Run Query:** Click the **Update Chart** (or **Run**) button.
4.  You should see a pie chart dividing your patients by Male/Female.

## Step 4: Save the Chart

1.  Click **Save** (top right).
2.  **Chart Name:** Enter "Patients by Gender".
3.  **Dashboard:** You can choose "Add to new dashboard" and name it "Patient Demographics".
4.  Click **Save & Go to Dashboard**.

## Other Ideas

Try creating these other charts with the same dataset:

- **Bar Chart:** Group by `geographic_location` to see where patients are coming from.
- **Box Plot:** Use `age` as the metric to see the age distribution.
