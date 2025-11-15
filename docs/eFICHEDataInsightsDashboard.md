

**fKey Metrics: Patient Demographics, Facility Distribution, and Procedure Language Trends**:

---

### **1️. Patient Distribution by Age Group**

```sql
SELECT
    CASE
        WHEN age < 18 THEN 'Child'
        WHEN age BETWEEN 18 AND 35 THEN 'Young Adult'
        WHEN age BETWEEN 36 AND 60 THEN 'Adult'
        ELSE 'Senior'
    END AS age_group,
    COUNT(*) AS total_patients,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) AS pct_of_total
FROM analytics.dim_patient
GROUP BY age_group
ORDER BY total_patients DESC;
```

Calculate percentage over all patients.

---

### **2️. Facility Usage by Region**

```sql
SELECT
    region,
    COUNT(facility_sk) AS total_facilities,
    ROW_NUMBER() OVER(PARTITION BY region ORDER BY COUNT(facility_sk) DESC) AS rank_in_region
FROM analytics.dim_facility
GROUP BY region
ORDER BY region, total_facilities DESC;
```

**ranked facilities per region**, useful for identifying regions with the most facilities.

---

### **3️. Top Languages Used in Procedures**

```sql
SELECT
    language_name,
    COUNT(*) AS usage_count,
    RANK() OVER(ORDER BY COUNT(*) DESC) AS rank_overall
FROM analytics.fact_language_usage
GROUP BY language_name
ORDER BY usage_count DESC
LIMIT 10;
```

**top 10 languages** used in reports/procedures.

---

### **4️. Patients with Most Encounters**

```sql
SELECT
    patient_code,
    total_encounters,
    RANK() OVER(ORDER BY total_encounters DESC) AS patient_rank
FROM analytics.dim_patient
WHERE total_encounters IS NOT NULL
ORDER BY patient_rank
LIMIT 10;
```

**frequent patients** and uses **RANK()** to handle ties.

---

### **5️. Language Usage With Audio vs Non-Audio**

```sql
SELECT
    language_name,
    SUM(CASE WHEN has_audio THEN 1 ELSE 0 END) AS audio_count,
    SUM(CASE WHEN NOT has_audio THEN 1 ELSE 0 END) AS text_only_count,
    COUNT(*) AS total_usage,
    ROUND(100.0 * SUM(CASE WHEN has_audio THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_audio
FROM analytics.fact_language_usage
GROUP BY language_name
ORDER BY total_usage DESC
LIMIT 10;
```

 Compares **audio vs text** usage of top languages. Very useful for analyzing content type.

---