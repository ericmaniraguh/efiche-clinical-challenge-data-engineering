This is the step by step **for PowerShell**, keeping everything inside the `efiche_assessment_eric_maniraguha` project - docker configuration.

---

## 1️. Create a **dedicated network** 

This isolates the project containers:

```powershell
docker network create efiche_network
```

All the containers will join this network, so you don’t need `--link` (links are legacy).

---

## 2️. PostgreSQL container

```powershell
docker run -d `
  --name airflow_postgres_new `
  --network efiche_network `
  -e POSTGRES_USER=postgres `
  -e POSTGRES_PASSWORD=admin `
  -e POSTGRES_DB=efiche_clinic_db_data_pipeline `
  -p 5434:5432 `
  -v airflow_pgdata_new:/var/lib/postgresql/data `
  postgres:15-alpine
```

Notes:

* Database is accessible at `localhost:5434`.
* Volume `airflow_pgdata_new` stores data persistently.

---

## 3️. Redis container

```powershell
docker run -d `
  --name efiche_redis `
  --network efiche_network `
  -p 6379:6379 `
  redis:7-alpine
```

Notes:

* Redis is on default port 6379.

---

## 4️. PgAdmin container

```powershell
docker run -d `
  --name efiche_pgadmin `
  --network efiche_network `
  -e PGADMIN_DEFAULT_EMAIL=admin@efiche.com `
  -e PGADMIN_DEFAULT_PASSWORD=admin `
  -p 5051:80 `
  dpage/pgadmin4:latest
```

## 4.2 Create Airflow Admin User

```
docker exec -it airflow_webserver airflow users create `
    --username airflow `
    --firstname Admin `
    --lastname Maniraguha `
    --role Admin `
    --email admin@efiche.com `
    --password admin

```

Notes:

* PgAdmin available at `http://localhost:5051`.
* Inside PgAdmin, connect to `airflow_postgres_new` (network name) using:

  * Host: `airflow_postgres_new`
  * Port: `5432`
  * User: `postgres`
  * Password: `admin`

---

## 5️. Airflow webserver

```powershell
docker run -d `
  --name airflow_webserver `
  --network efiche_network `
  -p 8083:8080 `
  -e AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://postgres:admin@airflow_postgres_new:5432/efiche_clinic_db_data_pipeline `
  -e AIRFLOW__CORE__EXECUTOR=SequentialExecutor `
  apache/airflow:2.10.2-python3.11 webserver
```

---

## 6️. Airflow scheduler

```powershell
docker run -d `
  --name airflow_scheduler `
  --network efiche_network `
  -e AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://postgres:admin@airflow_postgres_new:5432/efiche_clinic_db_data_pipeline `
  -e AIRFLOW__CORE__EXECUTOR=SequentialExecutor `
  apache/airflow:2.10.2-python3.11 scheduler
```

---

## 7️. Initialize Airflow DB

```powershell
docker exec -it airflow_webserver airflow db init
```

---

## 8️. Create Airflow admin user

```powershell
docker exec -it airflow_webserver airflow users create `
  --username airflow `
  --firstname Admin `
  --lastname User `
  --role Admin `
  --email admin@efiche.com `
  --password admin
```

Login Airflow at `http://localhost:8083` using `airflow / admin`.

---

### Info

1. All containers now live in **`efiche_network`**, so they can communicate via container names (no `--link` needed).
2. All persistent data is stored in Docker volumes (`airflow_pgdata_new` for Postgres).
3. Ports:

   * PostgreSQL: `5434`
   * PgAdmin: `5051`
   * Airflow: `8083`
   * Redis: `6379`

---
