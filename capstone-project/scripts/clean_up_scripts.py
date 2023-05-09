from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
import pandas as pd

import read_from_s3_and_write_to_parquet as RW

def SAS_time_to_datetime(spark, col_name, input_path, output_path):
    print(f'Reading data from {input_path}.')
    df = spark.read.parquet(input_path)
    print(f'Transforming {col_name}...')
    pandas_df = df.withColumn(col_name, col(col_name).cast('string')) \
              .toPandas()
    pandas_df[col_name] = pd.to_timedelta(pandas_df[col_name].astype(float), unit='D') + pd.Timestamp('1960-1-1')
    
    print(f'Writing data back to {output_path}.')
    spark.createDataFrame(pandas_df).write.parquet(output_path)

def main():    
    spark = RW.create_spark_session()
    
    SAS_time_to_datetime(spark, 'arrdate', 'sas_data', 'sas_data_cleaned')
    SAS_time_to_datetime(spark, 'depdate', 'sas_data_cleaned', 'sas_data_cleaned')
    
    describe_data(spark, 'sas_data_cleaned')
    
    
if __name__ == "__main__":
    main()