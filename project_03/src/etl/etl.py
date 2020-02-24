import configparser
import psycopg2
import boto3
from sql_queries import copy_table_queries, insert_table_queries, counting_queries


def load_staging_tables(cur, conn):
    """ Copy data from S3 into staging tables. 
    """
    for query in copy_table_queries:
        try:
            print("Loading staging table.")
            cur.execute(query)
            print("Successfully loaded staging table.")
        except psycopg2.Error as e:
            print("Error loading staging tables.")
            print(e)


def insert_tables(cur, conn):
    """ Insert data from staging tables into analytic tables (star schema).
    """
    for query in insert_table_queries:
        try:
            cur.execute(query)
            print("Inserted data to table.")
        except psycopg2.Error as e:
            print("Error inserting into tables.")
            print(e)


def count_queries(cur, conn):
    """ Function to get the row counts in the specified tables for data 
        integrity 
    """
    for query in counting_queries:
        try:
            print("Rows in table: ")
            cur.execute(query)
            row = cur.fetchone()
            if row is not None:
                print(row)
        except psycopg2.Error as e:
            print("Error inserting into tables.")
            print(e)


def main():
    """ Main function to load staging tables from S3 buckets, insert the staged data
        into analytic tables, and count the rows for data integrity.
    """

    # Get config data
    config = configparser.ConfigParser()
    config.read('../config/dwh.cfg')

    try:
        # Connect to dwh
        print("Create dwh connection...")
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
            *config['CLUSTER'].values()))
        conn.set_session(autocommit=True)
        cur = conn.cursor()
        print("Redshift connection completed")
    except psycopg2.Error as e:
        print("Error connecting and/or creating a cursor with db. ")
        print(e)

    # Load data from S3 into staging, insert the data into analytic tables,
    # and count the rows in the tables.
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    count_queries(cur, conn)

    # Close connection to dwh
    try:
        conn.close()
        print("Connection closed. ")
    except psycopg2.Error as e:
        print("Error closing connection to dwh. ")
        print(e)


if __name__ == "__main__":
    main()
