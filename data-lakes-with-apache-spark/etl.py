import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession

import extract_and_load as EL
import read_data_create_view as RC

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    
    """
        Create spark session for Spark SQL
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    
    """
    Read and create view for song data. Extract and load songs and artists tables.
    """
    
    RC.read_and_create_view_song_data(spark, input_data)

    EL.extract_and_load_songs(spark, output_data)
    EL.extract_and_load_artists(spark, output_data)
    
    
def process_log_data(spark, input_data, output_data):
    
    """
    Read and create view for log data. Extract and load users, time and songplays tables.
    """
    
    RC.read_and_create_view_log_data(spark, input_data)
    
    EL.extract_and_load_users(spark, output_data)
    EL.extract_and_load_time(spark, output_data)
    EL.extract_and_load_songplays(spark, output_data)


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend-melihburakmert/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
