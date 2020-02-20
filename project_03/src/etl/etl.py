import configparser
import psycopg2
import boto3
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        try:
            print("Loading staging table.")
            print("query: ", query)
            cur.execute(query)
            conn.commit()
            print("Successfully loaded staging table.")
        except psycopg2.Error as e:
            print("Error loading staging tables.")
            print(e)


def insert_tables(cur, conn):
    for query in insert_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error inserting into tables.")
            print(e)


def main():
    config = configparser.ConfigParser()
    config.read('../config/dwh.cfg')

    try:
        print("Create dwh connection")
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
            *config['CLUSTER'].values()))
        cur = conn.cursor()
        print("Redshift connection completed")
    except psycopg2.Error as e:
        print("Error connecting and/or creating a cursor with db. ")
        print(e)

    load_staging_tables(cur, conn)
    # insert_tables(cur, conn)

    # Close connection to dwh
    try:
        conn.close()
        print("Connection closed. ")
    except psycopg2.Error as e:
        print("Error closing connection to dwh. ")
        print(e)


if __name__ == "__main__":
    main()
