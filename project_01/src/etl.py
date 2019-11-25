import os
import glob
import psycopg2
import pandas as pd
import uuid
from sql_queries import *


def process_song_file(cur, filepath):
    """ Processes the song data files. Extracts, transforms, and loads via 
        dataframe. This function loads the data into the 'songs' and 
        'artists' tables. 
    """
    # open song file
    df = pd.read_json(filepath, typ='series')

    # insert song record
    song_data = (df.song_id, df.title, df.artist_id, df.year, df.duration)
    try:
        cur.execute(song_table_insert, song_data)
    except psycopg2.Error as e:
        print("Error inserting into songs table.")
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
    """ Processes the log data files. Extracts, transforms, and loads via dataframe.
        This function loads the data into the 'time', 'users', and 'songplays' 
        tables. 
    """

    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.loc[df['page'] == 'NextSong']
    # print("first df: ", df['ts'])

    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    # print("second df ", df['ts'].dt.week)

    # insert time data records
    time_data = []
    column_labels = ['timestamp', 'hour', 'day',
                     'week', 'month', 'year', 'weekday']

    # create time data list
    for row in df['ts']:
        time_data.append([row, row.hour, row.day, row.week,
                          row.month, row.year, int(row.weekday())])

    # create dataframe with aggregated data
    time_df = pd.DataFrame(time_data, columns=column_labels)

    # insert data into 'time' table from dataframe
    for i, row in time_df.iterrows():
        try:
            # pass
            cur.execute(time_table_insert, list(row))
        except psycopg2.Error as e:
            print("Error inserting time table row.")
            print(e)

    # load user table by selecting relevant dataframe columns
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records, use 'ON CONFLICT' clause for this. See sql_queries.py.
    for i, row in user_df.iterrows():
        # insert into user table
        try:
            cur.execute(user_table_insert, list(row))
        except psycopg2.Error as e:
            print("Error inserting user table row.")
            print(e)

    # Iterate over dataframe to insert songplay records
    for index, row in df.iterrows():
        # get songid and artistid from song and artist tables
        song_id, artist_id = None, None
        try:
            cur.execute(song_select, (row.artist, row.song, row.length,))
            # only one result per query possible
            song_row = cur.fetchone()
            if song_row != None:
                print("Match on row at index: ", index)
                # extract the matches into song and artist id
                try:
                    song_id, artist_id = song_row[0], song_row[2]
                except e:
                    print("Error populating song_id and artist_id with index.")
                    print(e)
        except psycopg2.Error as e:
            print("Error querying artist_id and song_id.")
            print(e)

        # create unique id for songplay_id
        songplay_id = str(uuid.uuid1())

        # insert songplay record
        songplay_data = (songplay_id, row.ts, row.userId, row.level,
                         song_id, artist_id, row.sessionId, row.location, row.userAgent)

        try:
            cur.execute(songplay_table_insert, songplay_data)
        except psycopg2.Error as e:
            print("Error inserting songplay table data.")
            print(e)


def process_data(cur, conn, filepath, func):
    """ Finds json file matches in directories, counts number of matched files,
        and processes each file by executing the function passed through the 
        'func' argument. 
    """
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
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """ Main function to run the script. Connects to sparkifydb and prepares 
        the cursor for queries. 
    """

    # connect to sparkify database
    try:
        conn = psycopg2.connect(
            "host=127.0.0.1 dbname=sparkifydb user=student password=student")
    except psycopg2.Error as e:
        print("Error creating database")
        print(e)

    # set commit to always true and create a cursor
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # Note that these are technically relative paths to project dir
    # since the run script executes this script from the parent dir.
    song_data_filepath = 'data/song_data'
    log_data_filepath = 'data/log_data'
    process_data(cur, conn, filepath=song_data_filepath,
                 func=process_song_file)
    process_data(cur, conn, filepath=log_data_filepath,
                 func=process_log_file)

    # close the connection
    conn.close()


#
# MAIN FUNCTION
#
if __name__ == "__main__":
    main()
