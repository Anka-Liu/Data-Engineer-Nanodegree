# AWS Data Warehousing Project

This project introduces Amazon Web Services and conducts good practice over S3 Bucket and AWS Redshift. 
This project builds the ETL pipeline to load json files of Sparkify user data from S3 Bucket into AWS Redshift and re-organize raw data into dimension and fact tables using PostgreSQL. 

## Introduction

Sparkify would like to move their processes and data onto AWS, and currently the data of user activities and songs resides in S3 Bucket in JSON format. We would like to create an analytical database for OLAP. The approach would be an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables. In addition, the analytical team is currently focusing on user activities of NextSong pages.

## Fact Tables & Dimension Table

The structure, tables, and columns are the same with those in [PostgreSQL Data Modeling Project]https://github.com/Anka-Liu/Data-Engineer-Nanodegree/tree/master/PostgreSQL%20Modeling.
