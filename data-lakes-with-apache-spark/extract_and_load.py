from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

def extract_and_load_songs(spark, output_data):
    
    """
    Extract columns to create songs table.
    Write songs table to parquet files partitioned by year and artist.
    """
    
    songs_table = spark.sql("""
        SELECT song_id, title, artist_id, year, duration
        FROM song_data
        WHERE song_id IS NOT NULL
    """)
    songs_output_path = output_data + 'songs/'
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(songs_output_path)
    
def extract_and_load_artists(spark, output_data):
    
    """
    Extract columns to create artists table.
    Write artists table to parquet files.
    """
    
    artists_table = spark.sql("""
        SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM song_data
        WHERE artist_id IS NOT NULL
    """)
    artists_output_path = output_data + 'artists/'
    artists_table.write.mode('overwrite').parquet(artists_output_path)
    
def extract_and_load_users(spark, output_data):
    
    """
    Extract columns for users table.
    Write users table to parquet files.
    """
    
    users_table = spark.sql("""
        SELECT DISTINCT
            t.userId as user_id,
            t.firstName as first_name,
            t.lastName as last_name,
            t.gender as gender,
            t.level as level
        FROM log_data t
        WHERE t.userId IS NOT NULL
    """)
    users_output_path = output_data + 'users/'
    users_table.write.mode('overwrite').parquet(users_output_path)
    
def extract_and_load_time(spark, output_data):
    
    """
    Extract columns to create time table.
    Write time table to parquet files partitioned by year and month
    """
    
    time_table = spark.sql("""
        SELECT 
            T.timestamp as start_time,
            hour(T.timestamp) as hour,
            dayofmonth(T.timestamp) as day,
            weekofyear(T.timestamp) as week,
            month(T.timestamp) as month,
            year(T.timestamp) as year,
            dayofweek(T.timestamp) as weekday
        FROM (
            SELECT to_timestamp(t_inner.ts/1000) as timestamp
            FROM log_data t_inner
            WHERE t_inner.ts IS NOT NULL
        ) T
    """)
    time_output_path = output_data + 'time/'
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(time_output_path)
    
def extract_and_load_songplays(spark, output_data):
    
    """
    Extract columns from joined song and log datasets to create songplays table.
    Write songplays table to parquet files partitioned by year and month
    """
    
    songplays_table = spark.sql("""
        SELECT
            monotonically_increasing_id() as songplay_id,
            to_timestamp(L.ts/1000) as start_time,
            month(to_timestamp(L.ts/1000)) as month,
            year(to_timestamp(L.ts/1000)) as year,
            L.userId as user_id,
            L.level as level,
            S.song_id as song_id,
            S.artist_id as artist_id,
            L.sessionId as session_id,
            L.location as location,
            L.userAgent as user_agent
        FROM log_data L
        JOIN song_data S on L.artist = S.artist_name and L.song = S.title
    """)
    songplays_output_path = output_data + 'songplays/'
    songplays_table.write.mode('overwrite').partitionBy('year', 'month').parquet(songplays_output_path)