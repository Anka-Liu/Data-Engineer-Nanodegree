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
Columns: firstName,lastName,song,userId 
Partition Keys: song, 

### songs
Columns: artist,firstName,itemInSession,lastName,sessionId,song,userId
Partition Keys: (sessionId, userId),itemInSession

### music   
Columns: artist,itemInSession,length,sessionId,song  
Partition Keys: sessionId, itemInSession

## Files

event_data: folder storing song and user activity data in .csv format.    
images: folder storing illustration of full table.    
event_datafile_new.csv: full table data produced by Project_1B_ Project_Template.ipynb.
Project_1B_ Project_Template.ipynb: main file of table creation and query execution. 

