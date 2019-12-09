# Project 2: Data Modeling with Apache Cassandra

Author: Ryan Anstotz

## Background

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.

They'd like a data engineer to create an Apache Cassandra database which can create queries on song play data to answer the questions, and wish to bring you on the project. Your role is to create a database for this analysis. You'll be able to test your database by running queries given to you by the analytics team from Sparkify to create the results.

## Summary

### Project Setup

To begin the project, data from the project workspace in the Jupyter notebook was compressed via tar and gzip and transferred it onto my local machine. Additionally, I installed Apache Cassandra and ran the local instance. The Python notebook was converted to a standard Python file (.py) for development.

All Python development is located in the `src/etl.py` file. All raw data is in `data/`.

### Development

This project required the creation of an ETL: pipeline to collect and consolidate data from multiple CSV files into a single CSV file for consumption. This file was saved as `event_datafile_new.csv`. Once the data was pre-processed, three queries were provided. A table was designed per each query. The queries were as follows:

    1.) Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4.

    2.) Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182.

    3.) Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'.

Each of these queries prints the results to the console. Please reference comments in code for additional information and details regarding the code and design of the system.

### Build Instructions

A run script (run.sh) was created to execute the code. Use `bash run.sh` to get the results.

### Results

The results per each query was as follows:

- Query 1:\n
  Faithless Music Matters (Mark Knight Dub) 495.30731201171875

- Query 2:\n
  Down To The Bone Keep On Keepin' On Sylvie Cruz\n
  Three Drives Greece 2000 Sylvie Cruz\n
  Sebastien Tellier Kilometer Sylvie Cruz\n
  Lonnie Gordon Catch You Baby (Steve Pitron & Max Sanna Radio Edit) Sylvie Cruz\n

- Query 3:\n
  Jacqueline Lynch\n
  Tegan Levine\n
  Sara Johnson\n
