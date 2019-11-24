import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath, typ='series')
    print("mydataframe: ", df)
    print("get song id: ", df.song_id)

    # insert song record
    song_data = (df.song_id, df.title, df.artist_id, df.year, df.duration)
    try:
        cur.execute(song_table_insert, song_data)
    except psycopg2.Error as e:
        print("Error inserting into songs table.")
        print("filepath: ", filepath)
        print(e)

    # insert artist record
    artist_data = (df.song_id, df.artist_name, df.artist_location,
                   df.artist_latitude, df.artist_longitude)

    try:
        cur.execute(artist_table_insert, artist_data)
    except psycopg2.Error as e:
        print("Error inserting into artists table.")
        print(e)


def process_log_file(cur, filepath):
    # open log file
    df = ''

    # filter by NextSong action
    df = ''

    # convert timestamp column to datetime
    t = ''

    # insert time data records
    time_data = ''
    column_labels = ''
    time_df = ''

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = ''

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        results = cur.execute(song_select, (row.song, row.artist, row.length))
        songid, artistid = results if results else None, None

        # insert songplay record
        songplay_data = ''
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        # conn.commit()  # don't think we need this w/ autocommit
        print('{}/{} files processed.'.format(i, num_files))


def main():
    # connect to sparkify database
    try:
        conn = psycopg2.connect(
            "host=127.0.0.1 dbname=sparkifydb user=student password=student")
    except psycopg2.Error as e:
        print("Error creating database")
        print(e)

    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # Note that these are technically relative paths to project dir
    # since the run script executes this script from the parent dir.
    song_data_filepath = 'data/song_data'
    log_data_filepath = 'data/log_data'
    process_data(cur, conn, filepath=song_data_filepath,
                 func=process_song_file)
    # process_data(cur, conn, filepath=log_data_filepath, func=process_log_file)

    conn.close()


#
# MAIN FUNCTION
#
if __name__ == "__main__":
    main()
