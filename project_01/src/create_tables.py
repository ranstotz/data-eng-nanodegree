import psycopg2
from sql_queries import create_table_queries, drop_table_queries, print_table


def create_database():
    """ Function to create the sparkifydb and return a 
        connection and cursor to it. """

    # connect to default database
    try:
        conn = psycopg2.connect(
            "host=127.0.0.1 dbname=studentdb user=student password=student")
    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres database")
        print(e)

    conn.set_session(autocommit=True)

    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get cursor to the Database")
        print(e)

    # create sparkify database with UTF8 encoding
    try:
        cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    except psycopg2.Error as e:
        print("Error dropping database")
        print(e)
    try:
        cur.execute(
            "CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")
    except psycopg2.Error as e:
        print("Error creating database")

    # close connection to default database
    conn.close()

    # connect to sparkify database
    try:
        conn = psycopg2.connect(
            "host=127.0.0.1 dbname=sparkifydb user=student password=student")
    except psycopg2.Error as e:
        print("Error creating database")
        print(e)

    conn.set_session(autocommit=True)
    cur = conn.cursor()
    return cur, conn


def drop_tables(cur, conn):
    """ Function to drop all tables prior to executing script
        to ensure old data is not used.
    """
    for query in drop_table_queries:
        # drop tables
        try:
            cur.execute(query)
        except psycopg2.Error as e:
            print("Error dropping table")
            print(e)


def create_tables(cur, conn):
    """ Function to create the tables provided in sql_queries.py.
    """
    for query in create_table_queries:
        # create tables
        try:
            cur.execute(query)
        except psycopg2.Error as e:
            print("Error creating table")
            print(e)


def main():
    """ Main function for create_tables.py. Handles connections
        to the database and executes relevant functions.
    """

    # create connection to database
    cur, conn = create_database()

    # ensure all old tables are dropped and new ones are created
    drop_tables(cur, conn)
    create_tables(cur, conn)

    # close connection
    conn.close()


#
# MAIN FUNCTION
#
if __name__ == "__main__":
    main()
