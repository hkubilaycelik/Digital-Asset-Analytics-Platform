Digital Asset Analytics Platform

1. Project Objective

This project is an end-to-end data engineering pipeline designed to handle high-frequency cryptocurrency data. The system architecture is built to be scalable for any digital asset, and this specific implementation processes approximately 9 GB dataset of raw BTCUSDT trades of may 2025 to demonstrate its capabilities. The goal is to transform this massive raw dataset into clean, aggregated, and valuable analytical models ready for business intelligence.

This portfolio piece demonstrates skills in cloud infrastructure management, large-scale data processing, data warehousing, and workflow orchestration.


2. Project Status

[x] Phase 1: Foundation & Ingestion
    [x] Infrastructure (S3 Data Lake) provisioned using Terraform.
    [x] Ingestion script created in Python with Boto3.
    [x] Approximately 9 GB of raw BTC trade data successfully uploaded to the S3 'raw/'.

[x] Phase 2: Data Processing & Transformation
    [x] Databricks environment configured and connected to the AWS account.
    [x] Raw trade data read and processed using a Databricks Notebook with PySpark.
    [x] Data aggregated from individual trades into 1-minute OHLCV candles.
    [x] Processed data successfully saved to the S3 'processed/' in Parquet format.

[ ] Phase 3: Warehousing & Modeling
    [ ] Load processed Parquet data from S3 into Snowflake.
    [ ] Model the data into analytical schemas using dbt.

[ ] Phase 4: Orchestration & Automation
    [ ] Develop an Airflow DAG to automate the entire pipeline from ingestion to modeling.

[ ] Phase 5: Visualization
    [ ] Connect Metabase to Snowflake to create analytical dashboards.




3. Tech Stack
Category	         Technology	                              Purpose
Cloud Provider	        AWS                            Hosting all cloud infrastructure.
Data Lake	            AWS S3	                       Storage for raw and processed data files.
IaC	                    Terraform	                   Provisioning and managing AWS resources as code.
Ingestion	            Python, Boto3	               Scripting the upload of source data to S3.
Data Processing	        Databricks, Apache Spark	   Large-scale transformation of raw data.
Data Warehouse	        Snowflake	                   (Upcoming) Storing structured data for BI.
Data Modeling	        dbt	(Upcoming)                 Transforming data within the warehouse.
Orchestration	        Apache Airflow	               (Upcoming) Automating and scheduling the pipeline.
Containerization	    Docker	                       Running Airflow in a consistent environment.
Visualization	        Metabase	                   (Upcoming) Building BI dashboards.
Data Formats	        CSV, Parquet	               I/O formats for raw and processed data.