# 🏠 Capstone Project: Real Estate Analytics Pipeline

![archetecture](<Screenshot 2025-06-02 at 02.27.15.png>)


## 🚀 Introduction

This capstone project implements a complete, production-ready data pipeline to analyze U.S. residential property data from the RentCast API. The system is designed to:

- Ingest and store real estate property data in a lakehouse architecture
- Model key metrics such as property valuations, taxes, and ownership
- Expose reliable datasets for analysis via dashboards and BI tools
- Ensure quality through validations and orchestrate through robust scheduling

---

## 📊 Dataset & Technology Justification

### 📦 Dataset: RentCast API
- Granular property information including location, features, tax history, and ownership
- API limit: 30 calls/day → handled via batching and retry logic

### ⚙️ Technologies Used

| Component         | Tool                         | Justification                                      |
|------------------|------------------------------|----------------------------------------------------|
| Ingestion         | Python + PyArrow              | Flexible API handling with batch support           |
| Storage Format    | Apache Iceberg + S3           | ACID, schema evolution, open format                |
| Data Warehouse    | Snowflake (external table)    | External table support via catalog                 |
| Transformation    | dbt                           | Modular, testable transformations                  |
| Orchestration     | Apache Airflow + Cosmos       | Robust, DAG-based orchestration                    |
| Secrets Management| AWS Secrets Manager           | Secure credential handling                         |
| Visualization     | Tableau Public                | Interactive dashboarding                           |
| Infrastructure    | Docker                        | Reproducible, isolated environment                 |

---

## 🔄 ETL Pipeline Flow

1. Extract data from RentCast API using credentials from AWS Secrets Manager.
2. Transform raw JSON to structured format and store in Apache Iceberg (S3).
3. Expose Iceberg table to Snowflake using external tables (Tabular catalog).
4. Transform and model using dbt:
   - Staging: `stg_property_data`
   - Dimensions: `dim_property_location`, `dim_property_structure`, `dim_owner`
   - Fact: `fact_property_valuation`
   - Report: `report_property_insights`
5. Visualize in Tableau

---

## 🧱 Schema Overview

### 🗃 Raw Schema (Iceberg)
40+ fields including: `property_id`, `city`, `state`, `zip_code`, `owner_names`, `value_2023`, etc.

### 🧩 Modeled Schemas (dbt)
- `stg_property_data`
- `dim_property_location`
- `dim_property_structure`
- `dim_owner`
- `fact_property_valuation`
- `report_property_insights`

---

##  Data Quality Tests Summary

| Model                    | Column        | Tests           |
|--------------------------|---------------|-----------------|
| `fact_property_valuation`| `PROPERTY_ID` | `not_null`, `unique` |
| `dim_property_location`  | `PROPERTY_ID` | `not_null`, `unique` |
| `dim_property_structure` | `PROPERTY_ID` | `not_null`, `unique` |
| `dim_owner`              | `PROPERTY_ID` | `not_null`, `unique` |
| `report_property_insights`| `PROPERTY_ID`| `not_null`             |

---

## 📸 Screenshots

### 🧭 Schema Diagram
![Schema Overview](./images/schema.png)

### 🗂 DAG Views
- ![Ingestion DAG](<Screenshot 2025-06-02 at 02.12.22.png>)
  (https://clokmwe5l00o101jjoe4dgf6j.astronomer.run/d75vqq1z/dags/api_ingestion_dag_v3/grid)
- ![Transformation DAG](<Screenshot 2025-06-02 at 02.11.51.png>)
  (https://clokmwe5l00o101jjoe4dgf6j.astronomer.run/d75vqq1z/dags/capstone_dbt_pipeline_split_v10/grid)
- ![Dags](<Screenshot 2025-06-02 at 02.12.50.png>)


### 📊 Tableau Dashboard
- [View on Tableau Public](https://public.tableau.com/authoring/capstoneproject_17488176068530/Propertydistributionmap/Dashboard%201#1)

- [Tableau](<Screenshot 2025-06-02 at 02.36.45-1.png>)


---

## 🧗 Challenges Faced

- **API Rate Limit:** Solved via batching and `MAX_PAGES` logic
- **Duplicate Records:** Addressed using `property_id` for deduplication
- **Zombie DAG Errors:** Fixed by increasing retries and setting `concurrency=1`

---

## 📬 Contact

contact stephen george 

