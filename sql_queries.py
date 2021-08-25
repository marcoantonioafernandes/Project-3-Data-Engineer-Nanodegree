import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events
(
    artist VARCHAR,
    auth VARCHAR, 
    firstName VARCHAR,
    gender VARCHAR,
    itemInSession integer, 
    lastName VARCHAR,  
    length DOUBLE PRECISION,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration DOUBLE PRECISION,
    sessionId INTEGER,
    song VARCHAR,
    status INTEGER,
    ts BIGINT,
    userAgent VARCHAR,
    userId INTEGER
)
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs
(
    num_songs BIGINT,
    artist_id VARCHAR, 
    artist_latitude DOUBLE PRECISION,
    artist_longitude DOUBLE PRECISION,
    artist_location VARCHAR, 
    artist_name VARCHAR, 
    song_id VARCHAR, 
    title VARCHAR, 
    duration DOUBLE PRECISION,
    year INTEGER
)
""")

songplay_table_create = ("""
CREATE TABLE songplays
(
    song_play_id BIGINT IDENTITY(1, 1) PRIMARY KEY NOT NULL,
    start_time TIMESTAMP NOT NULL REFERENCES time (start_time),
    user_id BIGINT NOT NULL  REFERENCES users (user_id), 
    level VARCHAR, 
    song_id VARCHAR REFERENCES songs (song_id), 
    artist_id VARCHAR REFERENCES artists (artist_id), 
    session_id INT, 
    location VARCHAR, 
    user_agent VARCHAR
)
""")

user_table_create = ("""
    CREATE TABLE  IF NOT EXISTS users (
        user_id BIGINT IDENTITY(1, 1) PRIMARY KEY NOT NULL,
        first_name VARCHAR NOT NULL,
        last_name VARCHAR,
        gender VARCHAR,
        level VARCHAR
    );
""")

song_table_create = ("""
    CREATE TABLE songs (
        song_id VARCHAR PRIMARY KEY NOT NULL, 
        title VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL REFERENCES artists (artist_id),
        year INT,
        duration decimal
    );
""")

artist_table_create = ("""
    CREATE TABLE artists (
    artist_id VARCHAR PRIMARY KEY NOT NULL,
    name VARCHAR NOT NULL,
    location VARCHAR,
    latitude decimal,
    longitude decimal
    )
""")

time_table_create = ("""
    CREATE TABLE  time (
        start_time TIMESTAMP PRIMARY KEY NOT NULL,
        hour INT,
        day INT,
        week INT, 
        month INT,
        year INT, 
        weekday INT
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    JSON {}
    compupdate off region 'us-west-2';
""").format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3','LOG_JSONPATH'))

staging_songs_copy =  ("""
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    delimiter ',' compupdate off region 'us-west-2';
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (
	start_time,  user_id, level, song_id, artist_id,  session_id,  location,  user_agent)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    VALUES
    (%s, %s, %s, %s, %s)
    ON CONFLICT (user_id) DO UPDATE
    SET level = EXCLUDED.level;
    """)

song_table_insert = ("""
    INSERT INTO songs (song_id,  title, artist_id, year, duration)
    VALUES
    (%s, %s, %s, %s, %s)
    ON CONFLICT (song_id) DO NOTHING;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    VALUES
    (%s, %s, %s, %s, %s)
    ON CONFLICT (artist_id) DO NOTHING;
""")


time_table_insert = ("""
    INSERT INTO time(start_time, hour, day, week, month, year, weekday)
    VALUES
    (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (start_time) DO NOTHING;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
