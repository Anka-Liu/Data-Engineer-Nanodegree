# AWS Data Warehousing Project

This project introduces Amazon Web Services and conducts good practice over S3 Bucket and AWS Redshift. 
This project builds the ETL pipeline to load json files of Sparkify user data from S3 Bucket into AWS Redshift and re-organize raw data into Star Schema consisting of dimension and fact tables using PostgreSQL. 

## Introduction

Sparkify would like to move their processes and data onto AWS, and currently the data of user activities and songs resides in S3 Bucket in JSON format. We would like to create an analytical database for OLAP. The approach would be an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables. In addition, the analytical team is currently focusing on user activities of NextSong pages.

## S3 Bucket Data

The data source consists of raw data files of two topics in JSON format, which comes from Udacity S3 Bucket:s3://udacity-dend/.

SONG_DATA(s3://udacity-dend/song_data): 
This folder contains JSON files about a song and the artist of that song, partitioned by the first three letters of each song's track ID.

LOG_DATA(s3://udacity-dend/log_data):
This folder contains JSON files about user activities on Sparkify app. In addition, the locations of log data are guided by JSONPATH file(s3://udacity-dend/log_json_path.json), used in the copy process from S3 Bucket to staging tables. 

## Fact Tables & Dimension Table

The schema, tables, and columns are the same with those in [PostgreSQL Data Modeling Project](https://github.com/Anka-Liu/Data-Engineer-Nanodegree/tree/master/PostgreSQL%20Modeling).

## Files

dwh.cfg: the configuration file containing Sparkify JSON data sources and AWS Redshift configuration   
sql_queries.py: the Python file storing pre-designed SQL commands of dropping, creating, loading, and inserting tables for the whole ETL process    
create_tables.py: the Python file to execute dropping and creating staging and final tables     
etl.py: the Python file to execute loading staging tables and inserting data into final tables

## How To Use

1. Set up AWS Redshift Cluster, IAM Role and IAM User for manipulation
2. Fill key configuration in dwh.cfg: 
- HOST for the endpoint address for Redshift Cluster
- DB_NAME for the Redshift Cluster name
- DB_USER for the master user name of Redshift Cluster
- DB_PASSWORD for the password of Redshift Cluster
- DB_PORT for the port code, which is defined by Security Group for Redshift Cluster TCP access
- ARN for the ARN of the corresponding IAM Role for Redshift Cluster
3. run create_tables.py
4. run test.ipynb to check database
