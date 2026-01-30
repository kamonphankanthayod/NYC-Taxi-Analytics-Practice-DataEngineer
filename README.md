# NYC Taxi Analytics Pipeline – From Colab to Cloud with Airflow

This repository demonstrates how an analytics notebook can be transformed
into a production-style data engineering pipeline.

The project was originally implemented as a **Google Colab notebook**
to answer an analytics question.
It was later **applied and extended** into a cloud-based, automated pipeline
using **Apache Airflow and AWS**.

---

## Project Background

The original task was part of an academic mini challenge
to analyze the number of NYC taxi trips by type
(Yellow, Green, FHV, FHVHV) for **January 2024**.

The initial solution was implemented in Google Colab as a standalone notebook.

To practice real-world data engineering concepts, I re-designed the solution
into an end-to-end pipeline with ingestion, transformation, orchestration,
analytics, and visualization using cloud services.

---

## Folder Explanation

- **google-colab/**
  - Original analytics notebook
  - Used to answer the problem directly using Python and pandas
  - Represents the starting point of the project

- **aws/**
  - Applied version of the project using cloud services
  - Implements ingestion, ETL, orchestration, and visualization
  - Designed to simulate a real-world data engineering workflow

---

## My Role

- Analyzed the original problem and notebook logic
- Re-designed the solution into a scalable data pipeline
- Implemented data ingestion using AWS Lambda
- Built ETL jobs with AWS Glue (PySpark)
- Orchestrated the workflow using Apache Airflow
- Created an analytics dashboard using Streamlit and Athena

---

## Pipeline Overview (Applied Version)

NYC Taxi Open Data  
→ AWS Lambda (Ingest)  
→ Amazon S3 (Raw Data Lake)  
→ AWS Glue Crawler  
→ AWS Glue ETL Job  
→ Amazon S3 (Processed Data)  
→ Amazon Athena  
→ Streamlit Dashboard  

All steps are orchestrated using **Apache Airflow**.

---

## Tech Stack

- Apache Airflow
- Python
- AWS S3
- AWS Lambda
- AWS Glue (PySpark)
- Amazon Athena
- Streamlit
- Docker & Docker Compose

---

## Data

- Source: NYC Taxi Open Dataset
- Format: Parquet
- Ingestion: Monthly
- Taxi Types:
  - Yellow
  - Green
  - FHV
  - FHVHV

---

## What This Project Demonstrates

- Translating an analytics notebook into a production-style pipeline
- Applying cloud services to automate data workflows
- Orchestrating ETL processes using Airflow
- Designing data lakes and analytics layers
- Building lightweight dashboards on top of cloud data

---

## Status

This is a personal learning and practice project.
The pipeline is manually triggered via the Airflow UI.
