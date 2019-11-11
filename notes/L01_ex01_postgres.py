#!/usr/bin/python3

# Lesson 1: Creating databases and tables in Postgres

import psycopg2

# Create a connection to the database
try:
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=demodb user=student password=student")
except psycopg2.Error as e:
    print("Error: Could not make connection to the Postgres database")
    print(e)

# Use the connection to get a cursor that can be used to execute queries
try:
    cur = conn.cursor()
except psycopg2.Error as e:
    print("Error: Could not get curser to the Database")
    print(e)

# auto commit so conn.commit() is not repeatedly called
conn.set_session(autocommit=True)

# Create a database to do work in
try:
    cur.execute("create database myDemoDb")
except psycopg2.Error as e:
    print(e)

# Create a Song Library that contains a list of songs, including the song name, artist name, year, album it was from, and if it was a single.
# song_title
# artist_name
# year
# album_name
# single
try:
    cur.execute("CREATE TABLE IF NOT EXISTS song_library (song_title varchar, artist_name varchar, year int, album_name varchar, single bool);")
except psycopg2.Error as e:
    print("Error: Issue creating table")
    print(e)

# TO-DO: Insert the following two rows in the table
# First Row:  "Across The Universe", "The Beatles", "1970", "False", "Let It Be"
# Second Row: "The Beatles", "Think For Yourself", "False", "1965", "Rubber Soul"
try:
    cur.execute("INSERT INTO song_library (album_name, artist_name, year, single, song_title) \
                 VALUES (%s, %s, %s, %s, %s)",
                ("Across The Universe", "The Beatles", "1970", "False", "Let It Be"))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print(e)

try:
    cur.execute("INSERT INTO song_library (artist_name, album_name, single, year, song_title) \
                  VALUES (%s, %s, %s, %s, %s)",
                ("The Beatles", "Think For Yourself", "False", "1965", "Rubber Soul"))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print(e)

# TO-DO: Validate your data was inserted into the table.
try:
    cur.execute("SELECT * FROM song_library;")
except psycopg2.Error as e:
    print("Error: select *")
    print(e)

row = cur.fetchone()
while row:
    print(row)
    row = cur.fetchone()

# And finally close your cursor and connection.
cur.close()
conn.close()
