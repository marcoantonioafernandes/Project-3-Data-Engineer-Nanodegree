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
    length FLOAT,
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
    num_songs INTEGER,
    artist_id VARCHAR, 
    artist_latitude DOUBLE PRECISION,
    artist_longitude DOUBLE PRECISION,
    artist_location VARCHAR, 
    artist_name VARCHAR, 
    song_id VARCHAR, 
    title VARCHAR, 
    duration FLOAT,
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
        user_id    INTEGER    NOT NULL  PRIMARY KEY,
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
    json 'auto' compupdate off region 'us-west-2';
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))
# FINAL TABLES
songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id ,level ,song_id, artist_id ,session_id ,location ,user_agent   )
                            SELECT timestamp 'epoch' + se.ts * interval '0.001 seconds' as start_time,
                           se.userId,
                           se.level,
                           ss.song_id,
                           ss.artist_id,
                           se.sessionId,
                           se.userAgent,
                           se.location
                           FROM staging_events se
                          JOIN staging_songs ss
                          ON se.artist=ss.artist_name
                          AND
                          se.song=ss.title
                         AND
                         se.length=ss.duration
                         WHERE se.page='NextSong'
                            
""")
user_table_insert = ("""INSERT INTO users( user_id   ,first_name ,  last_name , gender  ,level)
                    SELECT DISTINCT userId, firstName,lastName,gender,level
                    FROM staging_events
                    WHERE userId IS NOT NULL
                    AND page ='NextSong'
""")
song_table_insert = ("""INSERT INTO songs (song_id , title, artist_id, year, duration)
                         SELECT DISTINCT song_id, title,artist_id,year,duration
                        FROM staging_songs
                        WHERE song_id IS NOT NULL;
""")
artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude)
                          SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
                          FROM staging_songs 
                          WHERE artist_id is NOT NULL
""")
time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                        SELECT DISTINCT TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second' as start_time,
                        EXTRACT(HOUR FROM start_time) AS hour,
                        EXTRACT(DAY FROM start_time) AS day,
                        EXTRACT(WEEKS FROM start_time) AS week,
                        EXTRACT(MONTH FROM start_time) AS month,
                        EXTRACT(YEAR FROM start_time) AS year,
                        EXTRACT(WEEKDAY FROM start_time) AS weekday
                        
                       FROM staging_events
""")
# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]