#!/usr/bin/python3

# # Lesson 1 Exercise 2: Creating a Table with Apache Cassandra
# Running locally (I already installed, just start the service):
# http://cassandra.apache.org/doc/latest/getting_started/installing.html


# Import Apache Cassandra python package
import cassandra

# Create a connection to the database
from cassandra.cluster import Cluster
try:
    # If you have a locally installed Apache Cassandra instance
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
except Exception as e:
    print(e)


# Create a keyspace to do the work in (similar to database)
try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS udacity 
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
                    )

except Exception as e:
    print(e)

# Connect to the Keyspace
# Add in the keyspace you created
try:
    session.set_keyspace('udacity')
except Exception as e:
    print(e)

# Create a Song Library that contains a list of songs, including the song name, artist name, year, album it was from, and if it was a single.
#
# `song_title
# artist_name
# year
# album_name
# single`

# create a table to be able to run the following query:
# `select * from songs WHERE year=1970 AND artist_name="The Beatles"`

query = "CREATE TABLE IF NOT EXISTS songs"
query = query + \
    "(song_title text, artist_name text, album_name text, year int, single boolean, PRIMARY KEY (year, artist_name))"
try:
    session.execute(query)
except Exception as e:
    print(e)


# Insert the following two rows in your table
# `First Row:  "Across The Universe", "The Beatles", "1970", "False", "Let It Be"`
#
# `Second Row: "The Beatles", "Think For Yourself", "False", "1965", "Rubber Soul"`

# Add in query and then run the insert statement
query = "INSERT INTO songs (song_title, artist_name, album_name, year, single)"
query = query + " VALUES (%s, %s, %s, %s, %s)"

try:
    session.execute(query, ("Let It Be", "The Beatles",
                            "Across The Universe", 1970, False))

except Exception as e:
    print(e)

try:
    session.execute(query, ("Think For Yourself",
                            "The Beatles", "Rubber Soul", 1965, False))
except Exception as e:
    print(e)

# Validate your data was inserted into the table.
# Complete and then run the select statement to validate the data was inserted into the table
query = 'SELECT * FROM songs'
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

for row in rows:
    print(row.year, row.album_name, row.artist_name)


# Validate the Data Model with the original query.
#
# `select * from songs WHERE YEAR=1970 AND artist_name="The Beatles"`
query = "select * from songs where year=1970 and artist_name='The Beatles'"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

for row in rows:
    print(row.year, row.album_name, row.artist_name)

# close the session and cluster connection
session.shutdown()
cluster.shutdown()
