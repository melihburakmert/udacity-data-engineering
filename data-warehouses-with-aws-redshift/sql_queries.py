import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# GLOBAL VARIABLES
IAM_ROLE = config.get("IAM_ROLE","ARN")
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")

# Drop
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplays;"
user_table_drop = "DROP TABLE IF EXISTS dim_users;"
song_table_drop = "DROP TABLE IF EXISTS dim_songs;"
artist_table_drop = "DROP TABLE IF EXISTS dim_artists;"
time_table_drop = "DROP TABLE IF EXISTS dim_time;"

## Staging Tables
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist text,
    auth text, 
    firstName text,
    gender varchar(1),   
    itemInSession int,
    lastName text,
    length decimal,
    level varchar(10), 
    location text,
    method varchar(4),
    page varchar(10),
    registration bigint,
    sessionId int,
    song text,
    status int,
    ts timestamp,
    userAgent text,
    userId int);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs int,
    artist_id text,
    artist_latitude double precision,
    artist_longitude double precision,
    artist_location text,
    artist_name text,
    song_id text,
    title text,
    duration decimal,
    year int);
""")

## Final Tables
### Dimension Tables
user_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_users (
    user_id int PRIMARY KEY DISTKEY,
    first_name text,
    last_name text,
    gender varchar(1),
    level varchar(10));
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_songs (
    song_id text PRIMARY KEY,
    title text not null,
    artist_id text DISTKEY,
    year int,
    duration decimal not null);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_artists (
    artist_id text PRIMARY KEY DISTKEY,
    name text not null,
    location text,
    latitude double precision,
    longitude double precision);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_time (
    start_time timestamp PRIMARY KEY SORTKEY DISTKEY,
    hour int,
    day int,
    week int,
    month int,
    year int,
    weekday int);
""")

# Fact Tables
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS fact_songplays (
    songplay_id int IDENTITY(0,1) PRIMARY KEY SORTKEY,
    start_time timestamp not null,
    user_id int not null,
    level varchar(10),
    song_id text,
    artist_id text,
    session_id int,
    location text,
    user_agent text);
""")

# Staging Tables
staging_events_copy = ("""
    COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    region 'us-west-2'
    compupdate off 
    json {}
    TIMEFORMAT as 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(LOG_DATA, IAM_ROLE, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    region 'us-west-2'
    compupdate off
    json 'auto'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(SONG_DATA, IAM_ROLE)

# Staging to Final Inserts
user_table_insert = ("""
INSERT INTO dim_users(user_id, first_name, last_name, gender, level)
SELECT DISTINCT
    userId as user_id,
    firstName as first_name,
    lastName as last_name,
    gender,
    level
FROM staging_events
WHERE userId IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO dim_songs(song_id, title, artist_id, year, duration)
SELECT DISTINCT
    song_id,
    title,
    artist_id,
    year,
    duration
FROM staging_songs
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO dim_artists(artist_id, name, location, latitude, longitude)
SELECT DISTINCT
    artist_id,
    artist_name as name,
    artist_location as location,
    artist_latitude as latitude,
    artist_longitude as longitude
FROM staging_songs
WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO dim_time(start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT ts,
    EXTRACT(hour from ts),
    EXTRACT(day from ts),
    EXTRACT(week from ts),
    EXTRACT(month from ts),
    EXTRACT(year from ts),
    EXTRACT(weekday from ts)
FROM staging_events
WHERE ts IS NOT NULL;
""")

songplay_table_insert = ("""
INSERT INTO fact_songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT to_timestamp(to_char(se.ts, '9999-99-99 99:99:99'),'YYYY-MM-DD HH24:MI:SS'),
    se.userId as user_id,
    se.level as level,
    ss.song_id as song_id,
    ss.artist_id as artist_id,
    se.sessionId as session_id,
    se.location as location,
    se.userAgent as user_agent
FROM staging_events se
JOIN staging_songs ss ON se.artist = ss.artist_name AND se.song = ss.title;
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
