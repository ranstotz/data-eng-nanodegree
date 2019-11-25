# Project 01: Data Modeling with Postgres

Author: Ryan Anstotz

## Background

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They'd like a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis, and bring you on the project. Your role is to create a database schema and ETL pipeline for this analysis. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

## Summary

This project required the creation of an ETL pipeline to model song and log data using Postgres and Python using the psycopg2 module. A star schema was implemented using both fact and dimension tables. The fact table was 'songplays' whereas the dimension tables were 'songs', 'artists', 'users', and 'time'. The goal of this design was to optimize queries on song plays.

## Technical information

To accomplish this goal, SQL queries were made to create and drop the tables, and to insert records into the corresponding tables. These queries are located in sql_queries.py. This file also contains a function to view the first 5 records of a specified table for testing. Additionally, a SELECT statement with a JOIN is included as a helper for populating the songplays table.

The database and tables were generated via the create_tables.py file. This file first creates the Sparkify database (sparkifydb), then drops all tables to prevent duplicate data, creates the tables, then closes the database connection.

The data was extracted, transformed, and loaded through the etl.py file. Both song and log files were processed and loaded into the tables.

I did not utilize the Jupyter Notebook files. I developed within etl.py instead of its counterpart. Also, I wrote a custom function in sql_queries.py (print_table()) to replace what test.ipynb accomplished.

To execute my project please run `bash run.sh` from the 'project_01' directory. This is a driver script that runs create_tables.py, then etl.py and provides some simple print statements regarding execution progress.
