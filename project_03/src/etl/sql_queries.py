import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('../config/dwh.cfg')
IAM_ARN = config['IAM_ROLE']['ARN']
print("iam arn: ", IAM_ARN)
LOG_DATA_BUCKET = config['S3']['LOG_DATA']
SONG_DATA_BUCKET = config['S3']['SONG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']

# DROP TABLES
# temp remove staging drops to avoid the long wait
# staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
# staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS times"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist varchar,
    auth varchar,
    first_name varchar,
    gender char(1),
    item_session int,
    last_name varchar,
    length numeric,
    level varchar,
    location varchar,
    method varchar,
    page varchar,
    registration numeric,
    session_id int,
    song varchar,
    status int,
    ts bigint,
    user_agent varchar,
    user_id int
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    song_id varchar,
    num_songs int,
    artist_id varchar,
    artist_latitude numeric,
    artist_longitude numeric,
    artist_location varchar,
    artist_name varchar,
    title varchar,
    duration numeric,
    year int
)
""")
'''
staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs(
    song_id VARCHAR(100),
    num_songs INTEGER,
    artist_id VARCHAR(100),
    artist_latitude DOUBLE PRECISION,
    artist_longitude DOUBLE PRECISION,
    artist_location VARCHAR(255),
    artist_name VARCHAR(255),
    title VARCHAR(255),
    duration DOUBLE PRECISION,
    year INTEGER,
    PRIMARY KEY(song_id))
""")
'''

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id BIGINT IDENTITY(0,1) PRIMARY KEY,
    start_time timestamp,
    user_id varchar,
    level varchar,
    song_id varchar,
    artist_id varchar,
    session_id int,
    location varchar,
    user_agent varchar 
) 
""")


user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id varchar PRIMARY KEY,
    first_name varchar,
    last_name varchar,
    gender varchar,
    level varchar 
) 
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id varchar PRIMARY KEY,
    title varchar,
    artist_id varchar,
    year int,
    duration numeric
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar PRIMARY KEY,
    name varchar,
    location varchar,
    latitude numeric,
    longitude numeric
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time timestamp PRIMARY KEY,
    hour int,
    day int,
    week int,
    month int,
    year int,
    weekday int 
)
""")

# STAGING TABLES
# inserted into Redshift
staging_events_copy = ("""
copy staging_events from {}
iam_role '{}'
region 'us-west-2'
json {}
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
""").format(LOG_DATA_BUCKET, IAM_ARN, LOG_JSONPATH)

# inserted into Redshift
staging_songs_copy = ("""
copy staging_songs from {}
iam_role '{}'
region 'us-west-2'
json 'auto'
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
""").format(SONG_DATA_BUCKET, IAM_ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT TIMESTAMP 'epoch' + se.ts * INTERVAL '1 second' as start_time,
se.user_id,
se.level,
ss.song_id,
ss.artist_id,
se.session_id,
se.location,
se.user_agent
FROM staging_events se 
JOIN staging_songs ss 
ON se.song=ss.title AND se.artist=ss.artist_name

""")

# inserted into Redshift
user_table_insert = (""" 
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT user_id, first_name, last_name, gender, level
FROM staging_events
WHERE user_id IS NOT NULL
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs
WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs
WHERE artist_id IS NOT NULL
""")

# inserted into Redshift
time_table_insert = (""" 
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT TIMESTAMP 'epoch' + se.ts * INTERVAL '1 second' as start_time,
EXTRACT(hour from start_time),
EXTRACT(day from start_time),
EXTRACT(week from start_time),
EXTRACT(month from start_time),
EXTRACT(year from start_time),
EXTRACT(weekday from start_time)
FROM staging_events se
WHERE ts IS NOT NULL
""")

# Queries to count rows in tables

count_staging_songs = ("""
SELECT COUNT (*) AS cnt FROM staging_songs
""")

count_staging_events = ("""
SELECT COUNT (*) AS cnt FROM staging_events
""")

count_songs = ("""
SELECT COUNT (*) AS cnt FROM songs
""")

count_users = ("""
SELECT COUNT (*) AS cnt FROM users
""")

count_artists = ("""
SELECT COUNT (*) AS cnt FROM artists
""")

count_songplays = ("""
SELECT COUNT (*) AS cnt FROM songplays
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,
                        songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

# without events/songs
# create_table_queries = [songplay_table_create, user_table_create,
# song_table_create, artist_table_create, time_table_create]

# drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
#   songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

# without events/songs
drop_table_queries = [songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]


# copy_table_queries = [staging_events_copy, staging_songs_copy]
# don't copy for now
copy_table_queries = []

insert_table_queries = [songplay_table_insert, user_table_insert,
                        song_table_insert, artist_table_insert, time_table_insert]

# insert_table_queries = []


counting_queries = [count_staging_songs, count_staging_events,
                    count_songs, count_users, count_artists, count_songplays]
# temp_queries = []
