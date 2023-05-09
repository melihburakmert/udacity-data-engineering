import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession

import extract_and_load as EL
import create_view as RC

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    """
        Create spark session for Spark SQL
    """
    print('Creating Spark session..')
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.repositories", "https://repos.spark-packages.org/") \
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .enableHiveSupport() \
        .getOrCreate()
    
    print('Spark session is ready.')
    return spark

def process_state_data(spark, input_data, output_data):
    
    """
    Read and create view for state data. Extract and load state table.
    """
    print('Processing state data..')
    RC.read_and_create_view_state_data(spark, input_data)
    EL.extract_and_load_dim_state(spark, output_data)

def process_immigration_data(spark, input_data, output_data):
    
    """
    Read and create view for immigration data. Extract and load immigration table.
    """
    print('Processing immigration data..')
    RC.read_and_create_view_immigration_data(spark, input_data)
    EL.extract_and_load_fact_immigration(spark, output_data)
    
def process_temperature_data(spark, input_data, output_data):
    
    """
    Read and create view for temperature data. Extract and load temperature table.
    """
    print('Processing temperature data..')
    RC.read_and_create_view_temperature_data(spark, input_data)
    EL.extract_and_load_dim_temperature(spark, output_data)
    
def process_demographics_data(spark, input_data, output_data):
    
    """
    Read and create view for demographics data. Extract and load demographics table.
    """
    print('Processing demographics data..')
    RC.read_and_create_view_demographics_data(spark, input_data)
    EL.extract_and_load_dim_demographics(spark, output_data)


def main():
    spark = create_spark_session()
    
    input_path = "./staging_parquet/"
    output_data = "./output_parquet/"
    
    print('Starting ETL pipeline..')
    process_state_data(spark, input_path, output_data)
    process_immigration_data(spark, input_path, output_data)
    process_temperature_data(spark, input_path, output_data)
    process_demographics_data(spark, input_path, output_data)
    print('Done.')


if __name__ == "__main__":
    main()
