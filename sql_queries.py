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
    CREATE TABLE staging_events(
        artist_name VARCHAR,
        auth VARCHAR,
        first_name VARCHAR,
        gender  CHAR,
        item_in_session	INTEGER,
        last_name VARCHAR,
        song_length	FLOAT, 
        level VARCHAR,
        location VARCHAR,	
        method VARCHAR,
        page VARCHAR,	
        registration VARCHAR,	
        session_id	INTEGER,
        song_title VARCHAR,
        status INTEGER, 
        ts BIGINT,
        user_agent TEXT,	
        user_id INTEGER
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs(
        song_id VARCHAR,
        num_songs INTEGER,
        artist_id VARCHAR,
        artist_latitude FLOAT,
        artist_longitude FLOAT,
        artist_location VARCHAR,
        artist_name VARCHAR,
        title VARCHAR,
        duration FLOAT,
        year INTEGER
    )
""")

songplay_table_create = ("""
    CREATE TABLE songplays(
        songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
        start_time TIMESTAMP NOT NULL,
        user_id INTEGER NOT NULL,
        level VARCHAR NOT NULL,
        song_id VARCHAR,
        artist_id VARCHAR,
        session_id INTEGER NOT NULL,
        location VARCHAR NOT NULL,
        user_agent TEXT NOT NULL
    )
""")

user_table_create = ("""
    CREATE TABLE users(
        user_id INTEGER NOT NULL PRIMARY KEY,
        first_name VARCHAR NOT NULL,
        last_name VARCHAR NOT NULL,
        gender CHAR NOT NULL,
        level VARCHAR NOT NULL
    )
    diststyle all
""")

song_table_create = ("""
    CREATE TABLE songs(
        song_id VARCHAR PRIMARY KEY,
        title VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL,
        year INTEGER NOT NULL,
        duration FLOAT NOT NULL
    )
    diststyle all
""")

artist_table_create = ("""
    CREATE TABLE artists(
        artist_id VARCHAR PRIMARY KEY,
        name VARCHAR NOT NULL,
        location VARCHAR,
        latitude FLOAT,
        longitude FLOAT
    )
    diststyle all
""")

time_table_create = ("""
    CREATE TABLE time(
        start_time TIMESTAMP PRIMARY KEY sortkey distkey,
        hour INTEGER NOT NULL,
        day INTEGER NOT NULL,
        week INTEGER NOT NULL,
        month INTEGER NOT NULL,
        year INTEGER NOT NULL,
        weekday INTEGER NOT NULL
    )
""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events 
                          from {}
                          iam_role {}
                          json {};
                       """).format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""copy staging_songs 
                          from {} 
                          iam_role {}
                          json 'auto';
                      """).format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
    SELECT  
        to_timestamp(to_char(e.ts, '9999-99-99 99:99:99'),'YYYY-MM-DD HH24:MI:SS') AS start_time, 
        e.user_id, 
        e.level, 
        s.song_id,
        s.artist_id, 
        e.session_id,
        e.location, 
        e.user_agent
    FROM 
        staging_events e
    JOIN
        staging_songs s
    ON
        e.song_title = s.title 
    AND 
        e.artist_name = s.artist_name 
    AND 
        e.song_length = s.duration
    WHERE 
        e.page = 'NextSong' 
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT  
        user_id, 
        first_name, 
        last_name, 
        gender, 
        level
    FROM 
        staging_events
    WHERE 
        page = 'NextSong'
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration) 
    SELECT DISTINCT 
        song_id, 
        title,
        artist_id,
        year,
        duration
    FROM 
        staging_songs
    WHERE 
        song_id IS NOT NULL
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude) 
    SELECT DISTINCT 
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM 
        staging_songs
    WHERE 
        artist_id IS NOT NULL
""")

time_table_insert = ("""
    INSERT INTO time(start_time, hour, day, week, month, year, weekDay)
    SELECT DISTINCT
        start_time, 
        extract(hour from start_time),
        extract(day from start_time),
        extract(week from start_time), 
        extract(month from start_time),
        extract(year from start_time), 
        extract(dayofweek from start_time)
    FROM 
        songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
