PySpark + Databricks + dbt Data Engineering Project
📌 Project Overview
This project demonstrates an end-to-end data engineering workflow using PySpark, Databricks, and dbt.

The project focuses on ingesting raw data into Databricks, performing data transformations using PySpark, and applying additional transformations and data modeling using dbt. dbt snapshots were also used to load and maintain historical versions of the tables in Databricks.

Technologies Used
Python
PySpark
Databricks
dbt
SQL
Delta Lake
Git/GitHub
🏗️ Project Architecture
                 Raw Data
                    │
                    ▼
             ┌──────────────┐
             │   Databricks │
             │    Ingestion │
             └──────┬───────┘
                    │
                    ▼
             ┌──────────────┐
             │    PySpark   │
             │Transformations│
             └──────┬───────┘
                    │
                    ▼
            Databricks Tables
                    │
                    ▼
             ┌──────────────┐
             │     dbt      │
             │ Transformations
             └──────┬───────┘
                    │
                    ▼
              Trips Model
                    │
                    ▼
             ┌──────────────┐
             │ dbt Snapshots│
             │  Historical  │
             │    Tables    │
             └──────────────┘

🚀 Project Workflow
1. Data Ingestion
The raw datasets were ingested into Databricks using PySpark.

The ingestion process included:

Reading source data.
Loading raw data into Databricks.
Creating structured tables.
Handling data types and schemas.
Preparing data for downstream transformations.
2. Data Transformation Using PySpark
After ingestion, PySpark was used to perform data transformation and data preparation.

The transformations included activities such as:

Data cleansing.
Handling missing or invalid values.
Data type conversions.
Filtering unnecessary records.
Applying business transformations.
Creating structured datasets for downstream processing.
The transformed data was stored in Databricks for further processing.

🔄 dbt Transformations
dbt was used for SQL-based transformations on the data available in Databricks.

For this project, the main transformation implemented in dbt was the trips table/model.

The dbt layer was used to:

Transform the trips dataset.
Apply SQL-based business logic.
Create a clean and reusable data model.
Organize transformation logic within a dbt project.
Maintain transformation code in a version-controlled environment.
