from pyspark.sql import SparkSession
import configparser
import os
import sys

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

def read_sas_from_s3_and_write_to_parquet(spark, s3_bucket, parquet_path):
    print(f'Reading data from {s3_bucket}.')
    df_spark = spark.read \
        .format('com.github.saurfang.sas.spark') \
        .option("format", "sas7bdat") \
        .load('s3a://' + s3_bucket + '/*.sas7bdat')
    
    print(f'Writing parquet file to {parquet_path}.')
    df_spark.write.parquet(parquet_path)

def read_csv_from_s3_and_write_to_parquet(spark, s3_bucket, parquet_path, delimiter=','):
    print(f'Reading data from {s3_bucket}.')
    df_spark = spark.read \
        .format('csv') \
        .option("header", "true") \
        .option('sep', delimiter) \
        .load('s3a://' + s3_bucket + '/*.csv')
    
    print(f'Writing parquet file to {parquet_path}.')
    df_spark.write.parquet(parquet_path)
    
def read_csv_and_write_to_parquet(spark, input_path, parquet_path, delimiter=','):
    print(f'Reading data from {input_path}.')
    df_spark = spark.read.csv(input_path, header=True, sep=delimiter)
    
    print(f'Writing parquet file to {parquet_path}.')
    df_spark.write.parquet(parquet_path)

def describe_data(spark, parquet_path):
    print(f'Reading from {parquet_path}.')
    parquet_files = spark.read.load(parquet_path)
    parquet_files.printSchema()
    parquet_files.show(5)
    

def main():    
    spark = create_spark_session()
    
    read_csv_and_write_to_parquet(spark, 'data_sources/us-cities-demographics.csv', 'staging_parquet/demographics_data', ';')
    read_csv_and_write_to_parquet(spark, 'data_sources/GlobalLandTemperaturesByState.csv', 'staging_parquet/temperature_data')
    read_csv_and_write_to_parquet(spark, 'data_sources/states.csv', 'staging_parquet/state_data')
    
    describe_data(spark, 'sas_data')
    describe_data(spark, 'staging_parquet/demographics_data')
    describe_data(spark, 'staging_parquet/temperature_data')
    describe_data(spark, 'staging_parquet/state_data')
    
    
if __name__ == "__main__":
    main()