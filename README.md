
# Pinterest Data Pipeline Project

---

## Table of Contents:
- [Project Description](#project-description)
- [Installation](#installation)
- [Batch Processing](#batch-processing)
- [Kinesis Streaming](#kinesis-streaming)
- [Technologies Used](#technologies-used)
- [Usage](#usage)
- [File Structure](#file-structure)
- [What I Learned](#what-i-learned)
- [License](#license)

---

## Project Description:
This project implements a data pipeline to process real-time and batch data streams using AWS Kinesis, AWS MSK (Managed Streaming for Kafka), and Databricks. It aims to replicate Pinterest's experimental data pipeline, integrating both real-time stream processing and batch-oriented workflows. The pipeline is designed to ingest, process, and store data from various sources such as Pinterest, Geolocation, and User data. Apache Kafka and AWS Kinesis are used for event streaming, while Apache Spark handles data transformations and writing to Delta tables in Databricks.

---

## Installation:

1. **Databricks Setup**:
   - Create and configure a Databricks cluster for Spark processing.

2. **Install AWS SDK**:
   - Install the necessary AWS libraries, such as boto3, in your environment.

3. **AWS Credentials**:
   - Securely store and manage your AWS credentials within Databricks for accessing MSK and Kinesis.

4. **Kafka/MSK Setup**:
   - Configure an MSK cluster and Kafka topics for batch data ingestion.

---

## Batch Processing

The batch processing component of the pipeline begins by configuring an EC2 instance to run Kafka and setting up MSK (Managed Streaming for Kafka) to stream data to an S3 bucket. An API Gateway is then configured to send data to the MSK cluster, where Kafka topics are defined and used to capture data for batch processing. Databricks is integrated with MSK, allowing data from the S3 bucket to be mounted and processed using Apache Spark. Data cleaning and transformation steps are performed on the batch data, which includes Pinterest posts, geolocation data, and user data, before saving it to Delta tables for further analysis. This component also leverages AWS MWAA (Managed Workflows for Apache Airflow) to orchestrate the batch workflows in an automated fashion.

---

## Kinesis Streaming

In the real-time streaming component, AWS Kinesis Data Streams is configured to handle live data streams from multiple sources. An API Gateway is integrated with Kinesis to send real-time data to the streams. Databricks then consumes the streaming data from Kinesis and applies necessary transformations, including cleaning and formatting, to ensure the data is ready for storage. The cleaned and transformed data is then written to Delta tables, where it can be queried in real-time for further analysis. This ensures continuous data flow from the source through to the final storage layer.

---
## Technologies Used:

### Apache Kafka
Apache Kafka is an event streaming platform. It enables real-time data capture from event sources, stores event streams durably for retrieval, and processes them in real-time or retrospectively. Kafka ensures continuous data flow, which is crucial for this project’s batch processing pipeline.

### AWS MSK
Amazon Managed Streaming for Apache Kafka (MSK) is a fully managed Kafka service, used here to manage Kafka clusters and facilitate event stream ingestion and processing.

### AWS MSK Connect
MSK Connect simplifies the connection between Kafka clusters and external data sources like S3. It allows importing/exporting data between Kafka topics and other storage systems via pre-built connectors.

### Kafka REST Proxy
The Kafka REST Proxy provides a REST interface for interacting with a Kafka cluster. It simplifies producing and consuming messages in Kafka without needing to rely on native Kafka clients.

### AWS API Gateway
Amazon API Gateway manages and publishes APIs at scale. It acts as the "front door" for applications to access backend services, such as the Kafka cluster or other API-driven systems in this project.

### Apache Spark
Apache Spark is a distributed data processing engine, which is used in this project for both batch and streaming data processing. It integrates well with both Kinesis and Kafka, providing a unified platform for real-time and batch data analysis.

### PySpark
PySpark, the Python API for Spark, is used for real-time and large-scale data processing within Databricks. It combines the simplicity of Python with Spark’s powerful distributed processing engine.

### Databricks
Databricks is the platform used in this project for executing batch and streaming processes with Spark. It provides a unified environment for development, deployment, and management of data pipelines at scale.

### Managed Workflows for Apache Airflow (MWAA)
Apache Airflow orchestrates workflows and schedules batch processes. MWAA is used to automate batch processing tasks on Databricks.

### AWS Kinesis
AWS Kinesis is used for ingesting and processing real-time data streams. This project uses Kinesis Data Streams to temporarily store data, which is then processed by Spark in Databricks.

---

## Usage:

1. **Data Ingestion**:
   - Data from Kinesis and Kafka streams is ingested into Databricks using Spark.
   
2. **Data Cleaning**:
   - Real-time and batch data are cleaned, transformed, and processed in Databricks before storing them in Delta tables.

3. **Data Storage**:
   - Cleaned data is saved into Delta tables using the following names:
     - `12b83b649269_pin_table`
     - `12b83b649269_geo_table`
     - `12b83b649269_user_table`

4. **Running the Project**:
   - The pipeline is orchestrated with Apache Airflow (MWAA) for batch processing and Spark Structured Streaming for real-time data processing.

---

## File Structure:

/pinterest-data-pipeline652/ │ ├── .gitignore # File to specify intentionally untracked files ├── 0ec6d756577b_dag.py # Airflow DAG script for scheduling batch jobs ├── Kinesis Streaming 2024-09-23 11_48_09.ipynb # Jupyter Notebook for Kinesis streaming ├── Mount S3 bucket to databricks & Queries.ipynb # Jupyter Notebook for mounting S3 and running queries ├── README.md # Documentation for the project ├── user_posting_emulation.py # Script for simulating user post data ├── user_posting_emulation_streaming.py # Script for simulating streaming user post data

---

## What I Learned:
This project enhanced my understanding of:
- Real-time stream processing with AWS Kinesis.
- Batch processing with Apache Kafka and MSK.
- Data transformation and cleaning using PySpark in Databricks.
- Automating workflows using AWS MWAA and Delta Lake for efficient storage.

---

## License:
This project is licensed under the MIT License. Feel free to use, modify, and distribute the code as per the terms of the license.

---

## Final Thoughts:
This README will evolve as the project grows. Make sure to update it regularly as you implement new features or modify existing components.

---
