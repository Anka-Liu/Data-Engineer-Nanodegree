import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Execute copy commands in Redshift to copy raw data fro s3 bucket to staging_events & staging_songs.
    Input: 
    cur: cursor from database connection
    conn: database connection object to Redshift
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Execute insert commands in Redshift to distribute data to five dimension and fact tables.
    Input: 
    cur: cursor from database connection
    conn: database connection object to Redshift
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    #Read Redshift configuration from dwh.cfg
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    #Connect to Redshift via PostgreSQL and create cursor
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    #Execute loading and inserting functions
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    
    #Close Redshift Connection
    conn.close()


if __name__ == "__main__":
    main()
