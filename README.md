# NYC Taxi Analytics Pipeline – Airflow Practice Project

This repository showcases my hands-on practice in building an end-to-end
Data Engineering pipeline using Apache Airflow and AWS services.
The project simulates a real-world analytics workflow using NYC Taxi open data,
from ingestion to transformation and visualization.

This is part of the course CS341.

## Project Purpose
- Practice designing production-style data pipelines
- Learn Apache Airflow orchestration in a cloud environment
- Integrate multiple AWS services in a single workflow
- Build analytics-ready datasets and dashboards

## My Role
- Designed the overall data pipeline architecture
- Implemented data ingestion using AWS Lambda
- Built ETL jobs using AWS Glue (PySpark)
- Orchestrated workflows using Apache Airflow
- Created an analytics dashboard with Streamlit and Athena

## Pipeline Overview
NYC Taxi Open Data  
→ AWS Lambda (Ingest)  
→ Amazon S3 (Raw Data Lake)  
→ AWS Glue Crawler  
→ AWS Glue ETL Job  
→ Amazon S3 (Processed Data)  
→ Amazon Athena  
→ Streamlit Dashboard  

## Tech Stack
- Apache Airflow
- Python
- AWS S3
- AWS Lambda
- AWS Glue (PySpark)
- Amazon Athena
- Streamlit
- Docker & Docker Compose

## Data
- Source: NYC Taxi Open Dataset
- Format: Parquet
- Ingestion Frequency: Monthly
- Taxi Types: Yellow, Green, FHV, FHVHV

## Status
This is a personal practice project focused on learning and experimentation.
The pipeline is manually triggered via Airflow UI.
