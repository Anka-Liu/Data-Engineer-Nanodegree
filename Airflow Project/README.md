# Airflow Project

This project introducts the concepts of Apache Airflow & data pipeline control and utilize Amazon Redshift. This project builds the ETL pipeline based ont the automation and monitoring of Apache Airflow to load json files of Sparkify user data from S3 Bucket into Redshift and re-organize raw data into Star Schema consisting of dimension and fact tables.

## Introduction 

Sparkify would like to introduce data pipeline tools for user activities and songs dataset processing from AWS S3 Bucket to Amazon Redshift. We would like to create an analytical database with star schema. The tool would be Airflow to control AWS Redshift activities and ETL pipeline that extracts their data from S3 and transforms data into a set of dimensional tables. 

## S3 Bucket Data

The data source consists of raw data files of two topics in JSON format, which comes from Udacity S3 Bucket:s3://udacity-dend/.

SONG_DATA(s3://udacity-dend/song_data): This folder contains JSON files about a song and the artist of that song, partitioned by the first three letters of each song's track ID.

LOG_DATA(s3://udacity-dend/log_data): This folder contains JSON files about user activities on Sparkify app. In addition, the locations of log data are guided by JSONPATH file(s3://udacity-dend/log_json_path.json), used in the copy process from S3 Bucket to staging tables.

## Fact Tables & Dimension Table

The schema, tables, and columns are the same with those in [PostgreSQL Data Modeling Project](https://github.com/Anka-Liu/Data-Engineer-Nanodegree/tree/master/PostgreSQL%20Modeling).

## DAG Structure

## Files

create_tables: SQL queries of creating staging, fact, and dimension tables before starting data pipeline
dags: the folder containing dag definition and processing Python file, udac_dag.py
plugins: the folder containing SQL query and Operator files:   
- operators: containing packaged Python files of operator classes, including stage_redshift, load_fact, load_dimension, and data_quality
- helper: containing packaged sql_queries.py storing sql queries intended in the data pipeline

## How To Use

1. Initiate AWS User and Amazon Redshift for database connection
2. Create staging, fact, and dimension tables before starting data pipeline
3. Define sql query for data quality check. Please refer to run_quality_checks in udac_dag.py
4. Define dag settings like start_time, etc. Please refer to default_args  in udac_dag.py
4. Launch Airflow UI
5. Create aws_credentials and redshift as connection information in Connection
6. Start data pipeline
