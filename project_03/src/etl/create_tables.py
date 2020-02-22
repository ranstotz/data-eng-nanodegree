import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """ Function to drop all tables in provided in sql_queries.py.  """

    for query in drop_table_queries:
        try:
            print("Dropping table.")
            cur.execute(query)
            conn.commit()
            print("Dropped table.")
        except psycopg2.Error as e:
            print("Error executing drop table query.")
            print(e)


def create_tables(cur, conn):
    """ Function to create all tables in provided in sql_queries.py.  """

    for query in create_table_queries:
        try:
            print("Creating table.")
            cur.execute(query)
            conn.commit()
            print("Created table.")
        except psycopg2.Error as e:
            print("Error executing create table query.")
            print(e)


def main():
    """ 
    Main function for create_tables.py. This parses the config and
    creates a connection to the Redshift cluster based on config
    data. Then it drops tables and (re-) creates them. 
    """
    # Get config data
    config = configparser.ConfigParser()
    config.read('../config/dwh.cfg')

    try:
        print("Create Redshift connection.")
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
            *config['CLUSTER'].values()))
        cur = conn.cursor()
        print("Connected to Redshift successfully.")
    except psycopg2.Error as e:
        print("Error connecting and/or creating a cursor with db. ")
        print(e)

    # First drop tables if they exist, then create the tables
    # print("drop/create tables commented out since loaded.")
    drop_tables(cur, conn)
    create_tables(cur, conn)

    # Close connection to dwh
    try:
        conn.close()
        print("Connection closed. ")
    except psycopg2.Error as e:
        print("Error closing connection to dwh. ")
        print(e)


if __name__ == "__main__":
    main()
