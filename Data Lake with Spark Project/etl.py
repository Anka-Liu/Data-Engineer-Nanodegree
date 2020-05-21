import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    """
    Create Spark session.
    Input: None
    Return: Spark session.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Process song_data by reading .json files from song_data repository and processing songs_table and artists_table.
    Input:
        spark: Spark session
        input_data: location of input_data in .json files
        output_data: location specified for .parquet table outputs
    Return: None
    """
    # get filepath to song data file
    song_data = input_data+"song_data/*/*/*/"
    
    # read song data file
    df = spark.read.json(song_data)
    print('Read song_data successfully from {}.'.format(song_data))
    
    # extract columns to create songs table
    df.createOrReplaceTempView("song_data")
    songs_table = spark.sql("""
        select distinct song_id, title, artist_id, year, duration
        from song_data
        where song_id is not null
    """)
    print('Songs_table extracted successfully.')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data+'songs_table.parquet',mode='overwrite',partitionBy=['year','artist_id'])
    print('Songs_table wrote down successfully.')
    
    # extract columns to create artists table
    artists_table = spark.sql("""
        select distinct artist_id, 
               artist_name as name,
               artist_location as location, 
               artist_latitude as latitude, 
               artist_longitude as longitude
        from song_data
        where artist_id is not null
    """)
    print('Artists_table extracted successfully.')
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data+'artists_table.parquet',mode='overwrite')
    print('Artists_table wrote down successfully.')

def process_log_data(spark, input_data, output_data):
    """
    Process song_data by reading .json files from log_data repository and processing users_table,time_table, and songplays_table.
    Input:
        spark: Spark session
        input_data: location of input_data in .json files
        output_data: location specified for .parquet table outputs
    Return: None
    """
    # get filepath to log data file
    log_data = input_data+"log_data/*/*/"

    # read log data file
    df = spark.read.json(log_data)
    print('Read log_data successfully from {}.'.format(log_data))
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    print('Log_data filtered for NextSong.')

    # extract columns for users table  
    df.createOrReplaceTempView("log_data")
    users_table = spark.sql("""
        select distinct userId as user_id, 
               firstName as first_name,
               lastName as last_name, 
               gender, 
               level
        from log_data
        where userId is not null
    """)
    print('Users_table extracted successfully.')
    
    # write users table to parquet files
    users_table.write.parquet(output_data+'users_table.parquet',mode='overwrite')
    print('Users_table wrote down successfully.')
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000.0).strftime('%Y-%m-%d %H:%M:%S'))
    spark.udf.register("get_datetime",get_datetime)
    df = spark.sql("""
        select *, get_datetime(ts) as datetime
        from log_data
    """)
    print('Get_datetime function registered successfully.')
    
    # extract columns to create time table
    df.createOrReplaceTempView("time_table")
    time_table = spark.sql("""
        select distinct datetime as start_time,
               hour(datetime) as hour,
               day(datetime) as day,
               weekofyear(datetime) as week,
               month(datetime) as month,
               year(datetime) as year,
               dayofweek(datetime) as weekday
        from time_table
        where datetime is not null
    """)
    print('Time_table extracted successfully.')
    
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data+'time_table.parquet',mode='overwrite',partitionBy=['year','month'])
    print('Time_table wrote down successfully.')

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data+'song_data/*/*/*/')
    print('Read song_data successfully from {}.'.format(input_data+'song_data/*/*/*/'))
    
    # extract columns from joined song and log datasets to create songplays table 
    song_df.createOrReplaceTempView("song_data")
    songplays_table = spark.sql("""
        select row_number() over(order by datetime) as songplay_id,
               datetime as start_time,
               userId as user_id,
               level,
               song_id,
               artist_id,
               sessionId as session_id,
               location,
               userAgent as user_agent
        from time_table as t
        join song_data as s
        on t.song = s.title and 
           t.artist = s.artist_name and
           t.length = s.duration
    """)
    print('Songplays_table extracted successfully.')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data+'songplays_table.parquet',mode='overwrite')
    print('Songplays_table wrote down successfully.')


def main():
    spark = create_spark_session()
    input_data = "s3://udacity-dend/"
    output_data = "output_data/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()