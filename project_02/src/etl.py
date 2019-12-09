#!/usr/bin/python3
# coding: utf-8

# Import Python packages
import sys
import os
from cassandra.cluster import Cluster
import cassandra
import pandas as pd
import numpy as np
import re
import glob
import json
import csv

# ====================
# Part I. ETL Pipeline for Pre-Processing the Files
#

# Get current folder and subfolder event data
# Since the run.sh script is executed from the parent folder, consider
# the getcwd() as the parent dir.
filepath = os.getcwd() + '/data'

# Create a for loop to create a list of files and collect each filepath
for root, dirs, files in os.walk(filepath):

    # join the file path and roots with the subdirectories using glob
    file_path_list = glob.glob(os.path.join(root, '*'))
    # print(file_path_list)


# Process the files to create the single data file csv that will be used for Apache Casssandra tables
# initiating an empty list of rows that will be generated from each file
full_data_rows_list = []

# for every filepath in the file path list
for f in file_path_list:

    # reading csv file
    with open(f, 'r', encoding='utf8', newline='') as csvfile:
        # creating a csv reader object
        csvreader = csv.reader(csvfile)
        # remove the first row
        next(csvreader)

        # extracting each data row one by one and append it
        for line in csvreader:
            # print(line)
            full_data_rows_list.append(line)


# Creat a smaller event data csv file called event_datafile_full csv that will
# be used to insert data into the Apache Cassandra tables
csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

with open('event_datafile_new.csv', 'w', encoding='utf8', newline='') as f:
    writer = csv.writer(f, dialect='myDialect')
    writer.writerow(['artist', 'firstName', 'gender', 'itemInSession', 'lastName',
                     'length', 'level', 'location', 'sessionId', 'song', 'userId'])
    for row in full_data_rows_list:
        if (row[0] == ''):
            continue
        writer.writerow((row[0], row[2], row[3], row[4], row[5],
                         row[6], row[7], row[8], row[12], row[13], row[16]))

# ====================
# Part II. Complete the Apache Cassandra coding portion of your project.
#

# Create connection to the database and a session
try:
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
except Exception as e:
    print(e)

# Create a keyspace named project_02
try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS project_02
    WITH REPLICATION =
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
                    )
except Exception as e:
    print(e)

# Connect to keyspace
try:
    session.set_keyspace('project_02')
except Exception as e:
    print(e)

# ==========
# Query 1:  Give me the artist, song title and song's length in the
# music app history that was heard during sessionId = 338, and itemInSession = 4
#

print("\nQuery 1 results: \n")

# Ensure table is dropped prior to creation.
query = "DROP TABLE IF EXISTS music_library_0"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)


# Create music library for query 1
query = "CREATE TABLE IF NOT EXISTS music_library_0 "
query = query + "(session_id int, item_in_session int, artist_name text, \
                song_title text, song_length float, PRIMARY KEY (session_id, \
                item_in_session))"

try:
    session.execute(query)
except Exception as e:
    print(e)

# We have provided part of the code to set up the CSV file. Please complete the
# Apache Cassandra code below
file = 'event_datafile_new.csv'

with open(file, encoding='utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader)  # skip header
    for line in csvreader:
        # Create INSERT statement
        query = "INSERT INTO music_library_0 (session_id, item_in_session, artist_name, \
            song_title, song_length)"
        query = query + "VALUES (%s, %s, %s, %s, %s)"
        # Assign columns for INSERT query.
        session.execute(query, (int(line[8]), int(
            line[3]), line[0], line[9], float(line[5])))

# Verify the data was entered into the table by executing query
query = "select artist_name, song_title, song_length from music_library_0 \
    WHERE session_id=338 AND item_in_session=4"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

for row in rows:
    print(row.artist_name, row.song_title, row.song_length)

# ==========
# Query 2: Give me only the following: name of artist, song (sorted by itemInSession)
# and user (first and last name) for userid = 10, sessionid = 182
#

print("\nQuery 2 results: \n")

# Ensure table is dropped prior to creation.
query = "DROP TABLE IF EXISTS music_library_1"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)


# Create music library for query 2. Add item_in_session to primary key to sort by it
query = "CREATE TABLE IF NOT EXISTS music_library_1 "
query = query + "(session_id int, user_id int, item_in_session int, artist_name text, song_title text, \
                user_first_name text, user_last_name text, PRIMARY KEY (session_id, user_id, \
                    item_in_session))"

try:
    session.execute(query)
except Exception as e:
    print(e)

# We have provided part of the code to set up the CSV file. Please complete the
# Apache Cassandra code below
file = 'event_datafile_new.csv'

with open(file, encoding='utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader)  # skip header
    for line in csvreader:
        # Create INSERT statement
        query = "INSERT INTO music_library_1 (session_id, user_id, item_in_session, artist_name, song_title, \
                user_first_name, user_last_name)"
        query = query + "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        # Assign columns for INSERT query.
        session.execute(query, (int(line[8]), int(line[10]), int(line[3]),
                                line[0], line[9], line[1], line[4]))

# Verify the data was entered into the table by executing query
query = "select artist_name, song_title, user_first_name, user_last_name from music_library_1 \
    WHERE session_id=182 AND user_id=10"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

for row in rows:
    print(row.artist_name, row.song_title,
          row.user_first_name, row.user_last_name)

# ==========
# Query 3: Give me every user name (first and last) in my music app history who
# listened to the song 'All Hands Against His Own'
#

print("\nQuery 3 Results: \n")

# Ensure table is dropped prior to creation.
query = "DROP TABLE IF EXISTS music_library_2"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)


# Create music library for query 3
query = "CREATE TABLE IF NOT EXISTS music_library_2 "
query = query + "(song_title text, user_id int, user_first_name text, \
                    user_last_name text, PRIMARY KEY (song_title, user_id))"

try:
    session.execute(query)
except Exception as e:
    print(e)

# Data file for consumption
file = 'event_datafile_new.csv'

with open(file, encoding='utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader)  # skip header
    for line in csvreader:
        # Create INSERT statement
        query = "INSERT INTO music_library_2 (song_title, user_id, \
                user_first_name, user_last_name)"
        query = query + "VALUES (%s, %s, %s, %s)"
        # Assign columns for INSERT query.
        session.execute(query, (line[9], int(line[10]), line[1], line[4]))

# Verify the data was entered into the table by executing query
query = "select user_first_name, user_last_name from music_library_2 \
    WHERE song_title='All Hands Against His Own'"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

for row in rows:
    print(row.user_first_name, row.user_last_name)


# Drop the tables
query = "DROP TABLE IF EXISTS music_library_0"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

query = "DROP TABLE IF EXISTS music_library_1"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

query = "DROP TABLE IF EXISTS music_library_2"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

# Close the session and cluster connections
session.shutdown()
cluster.shutdown()
