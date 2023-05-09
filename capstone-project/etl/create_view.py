def read_and_create_view_state_data(spark, input_path):
    
    """
    Create path for state staging data. Read data in dataframe.
    Create table view for SQL.
    """
    
    df = spark.read.parquet(input_path + '/state_data')
    df.createOrReplaceTempView('state_data')

def read_and_create_view_immigration_data(spark, input_path):
    
    """
    Create path for immigration staging data. Read data in dataframe.
    Create table view for SQL.
    """
    
    df = spark.read.parquet(input_path + '/immigration_data')
    df.createOrReplaceTempView('immigration_data')
    
def read_and_create_view_demographics_data(spark, input_path):
    
    """
    Create path for demographics staging data. Read data in dataframe.
    Create table view for SQL.
    """
    
    df = spark.read.parquet(input_path + '/demographics_data')
    df.createOrReplaceTempView('demographics_data')
    
def read_and_create_view_temperature_data(spark, input_path):
    
    """
    Create path for temperature data. Read data in dataframe and filter.
    Create table view for SQL.
    """
    
    df = spark.read.parquet(input_path + '/temperature_data')
    df = df.filter(df.Country == 'United States')
    df.createOrReplaceTempView('temperature_data')