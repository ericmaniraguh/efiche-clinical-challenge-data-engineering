

# eFICHE Clinical Data Warehouse – Take-Home Assessment

### **Assessment Completion Summary**

I built a production-ready clinical data warehouse for eFiche, covering the full pipeline from modeling to analytics:

* **Data Model:** Designed a scalable PostgreSQL schema with patients, encounters, procedures, reports, and diagnoses. Included an ERD.
* **Synthetic Data:** Generated over 20,000 synthetic clinical records using PadChest metadata and Hugging Face techniques.
* **Pipeline:** Implemented a Dockerized ETL pipeline using Python and Airflow that fetches, transforms, and loads data incrementally while managing duplicates.
* **Data Warehouse:** Built a star schema with fact and dimension tables for optimized analytics.
* **Analytics:** Delivered SQL queries for key metrics (e.g., encounters/month, diagnosis trends, avg studies/patient) and materialized views with fast response times.
* **Quality & Deployment:** Added audit logic, validation steps, and documentation. Pipeline is fully reproducible with Docker Compose.

---



