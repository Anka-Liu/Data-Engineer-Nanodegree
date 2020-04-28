# PostgreSQL Data Modeling Project

This project is to build ETL pipeline to load json files of Sparkify user data into PostgreSQL database. The database follows star schema, with fact table as songplays connecting to dimensional tables of songs, artists, users, time. 

## Introduction

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## Fact Table

### songplays

records in log data associated with song plays i.e. records with page NextSong.  
Source: dimension tables and log_data  
Columns: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent  

## Dimension Tables

### users
users in the app.     
Source: log_data  
Columns: user_id, first_name, last_name, gender, level  

### songs
songs in music database.   
Source: songs_data.   
Columns: song_id, title, artist_id, year, duration   

### artists
artists in music database.   
Source: songs_data    
Columns: artist_id, name, location, latitude, longitude   

### time
timestamps of records in songplays broken down into specific units.   
Source: log_data   
Columns: start_time, hour, day, week, month, year, weekday     

## Files

data: folder storing song and user activity data in .json format.    
create_tables.py: clean database and create new fact and dimension tables.    
etl.ipynb: experiments of etl pipeline code.   
etl.py: conduct etl pipeline to insert records from json files in data folder.    
sql_queries.py: storage of SQL create/insert/drop commands.    
test.ipynb: test whether the tables are created and filled.    

## How To Use

1. run create_tables.py  
2. run etl.py  
3. run test.ipynb to check database
