def read_and_create_view_song_data(spark, input_data):
    
    """
    Create path for song data. Read data in dataframe.
    Create table view for SQL.
    """
    
    song_data = input_data + 'song_data/*/*/*/*.json'
    df = spark.read.json(song_data)
    df.createOrReplaceTempView('song_data')
    
def read_and_create_view_log_data(spark, input_data):
    
    """
    Create path for log data. Read data in dataframe and filter.
    Create table view for SQL.
    """
    
    log_data = input_data + 'log_data/*/*/*.json'
    df = spark.read.json(log_data)
    df = df.filter(df.page == 'NextSong')
    df.createOrReplaceTempView('log_data')