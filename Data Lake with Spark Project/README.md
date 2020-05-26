# Data Lake with Spark Project

This project introduces the concepts of Data Lake and Spark and utilize AWS ElasticMapReduce(EMR). 
This project builds the ETL pipeline to load json files of Sparkify user data from S3 Bucket into AWS EMR via Spark 
and re-organize raw data into Star Schema consisting of dimension and fact tables using Spark SQL. The final tables will
be stored in .parquet format.

## Introduction

Sparkify would like to move their processes and data onto AWS, and currently the data of user activities and songs resides
in S3 Bucket in JSON format. We would like to create an analytical database for OLAP. The approach would be an ETL pipeline 
that extracts their data from S3 and transforms data into a set of dimensional tables using Spark cluster on AWS EMR. 
In addition, the analytical team is currently focusing on user activities of NextSong pages.

## S3 Bucket Data

The data source consists of raw data files of two topics in JSON format, which comes from Udacity S3 Bucket:s3://udacity-dend/.

SONG_DATA(s3://udacity-dend/song_data): This folder contains JSON files about a song and the artist of that song, partitioned
by the first three letters of each song's track ID.

LOG_DATA(s3://udacity-dend/log_data): This folder contains JSON files about user activities on Sparkify app. In addition, the 
locations of log data are guided by JSONPATH file(s3://udacity-dend/log_json_path.json), used in the copy process from S3 Bucket 
to staging tables.

## Fact Tables & Dimension Table

The schema, tables, and columns are the same with those in [PostgreSQL Data Modeling Project](https://github.com/Anka-Liu/Data-Engineer-Nanodegree/tree/master/PostgreSQL%20Modeling).

## Files

dl.cfg: the configuration file storing AWS user & secret key for AWS connection.
etl.py: the Python file to execute the whole ETL process from reading .json files to outputting .parquet files.
data: the folder storing sample .json files of song_data and log_data, available for sample ETL processing test.

## How To Use

1. Complete AWS user & secret key in dl.cfg for AWS connection.
2. Run etl.py via spark-submit in AWS CLI connection to AWS EMR
3. Or alternatively, copy the code in etl.py and execute the code in PySpark environment in EMR Notebook
