import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drop staging, fact, and dimension tables.
    Input: 
    cur: cursor from database connection
    conn: database connection object to Redshift
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Create staging, fact, and dimension tables.
    Input: 
    cur: cursor from database connection
    conn: database connection object to Redshift
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    #Read Redshift configuration from dwh.cfg
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    #Connect to Redshift via PostgreSQL and create cursor
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    #Execute table dropping and creating functions
    drop_tables(cur, conn)
    create_tables(cur, conn)
    
    #Close Redshift Connection
    conn.close()


if __name__ == "__main__":
    main()
