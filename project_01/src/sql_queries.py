import psycopg2

# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays \
    (songplay_id varchar PRIMARY KEY, start_time timestamp, user_id varchar, level varchar, \
        song_id varchar, artist_id varchar, session_id int, location varchar, \
            user_agent varchar)
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users \
    (user_id varchar PRIMARY KEY, first_name varchar, last_name varchar, gender varchar, \
        level varchar)
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (song_id varchar PRIMARY KEY, title varchar, artist_id varchar, year int, duration numeric)
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists \
    (artist_id varchar PRIMARY KEY, name varchar, location varchar, latitude numeric, \
        longitude numeric)
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time \
    (start_time timestamp PRIMARY KEY, hour int, day int, week int, month int, year int, \
        weekday int )
""")

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays (songplay_id, start_time, user_id, level, song_id, artist_id, \
        session_id, location, user_agent) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
""")

# Use upsert clause to prevent duplicate inserts. This was written for log file reads.
user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level) VALUES (%s, %s, %s, %s, %s) \
    ON CONFLICT (user_id) DO NOTHING
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration) VALUES (%s, %s, %s, %s, %s)
""")

artist_table_insert = ("""INSERT INTO artists \
    (artist_id, name, location, latitude, longitude) VALUES (%s, %s, %s, %s, %s)
""")

time_table_insert = ("""INSERT INTO time \
    (start_time, hour, day, week, month, year, weekday) VALUES (%s, %s, %s, %s, %s, %s, %s) \
        ON CONFLICT (start_time) DO NOTHING
""")


def print_table(cur, table_name):
    """ Function to print the contents of table ensuring inserts were accurately executed. """
    query = "SELECT * FROM " + table_name + " LIMIT 5;"
    try:
        cur.execute(query)
    except psycopg2.Error as e:
        print("Error printing table w/ SELECT *")
        print(e)
    row = cur.fetchone()
    while row:
        print(row)
        row = cur.fetchone()


# FIND SONGS
song_select = ("""SELECT * FROM songs JOIN artists ON artists.name=%s WHERE songs.title=%s AND songs.duration=%s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create,
                        song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]
