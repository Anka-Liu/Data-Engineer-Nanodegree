# Cassandra Data Modeling Project

This project is to build ETL pipeline to process .csv files, combine a final table of Sparkify user data, and insert tables into Cassandra database based on query needs. 3 tables are created according to 3 different queries. 

## Full table Example

<img src="images/image_event_datafile_new.jpg">

## Tables Created

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

