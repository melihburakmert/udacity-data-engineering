def extract_and_load_fact_immigration(spark, output_data):
    
    """
    Extract columns to create immigration table.
    Write immigration table to parquet files partitioned by state_code.
    """
    
    fact_immigration_table = spark.sql("""
        SELECT
            CAST(cicid AS INT) AS cicid,
            DATE_ADD(CAST('1960-01-01' AS DATE), arrdate) AS arrival_date,
            DATE_ADD(CAST('1960-01-01' AS DATE), depdate) AS departure_date,
            CAST(i94cit AS INT) AS citizenship_code,
            CAST(i94res AS INT) AS residency_code,
            i94addr AS state_code,
            CAST(i94visa AS INT) AS i94_visa_type,
            visatype AS temp_visa_type
        FROM
            immigration_data
    """)
    output_path = output_data + 'fact_immigration/'
    fact_immigration_table.write.mode('overwrite').partitionBy("state_code").parquet(output_path)
    
def extract_and_load_dim_state(spark, output_data):
    
    """
    Extract columns to create state table.
    Write temperature table to parquet files.
    """
    
    dim_state_table = spark.sql("""
        SELECT DISTINCT
            State_code AS state_code,
            State AS state
        FROM
            state_data
    """)
    output_path = output_data + 'dim_state/'
    dim_state_table.write.mode('overwrite').parquet(output_path)
    
def extract_and_load_dim_temperature(spark, output_data):
    
    """
    Extract columns to create temperature table.
    Write temperature table to parquet files partitioned by state_code.
    """
    
    dim_temperature_table = spark.sql("""
        SELECT
            TO_DATE(td.dt, 'yyyy-MM-dd') AS dt,
            CAST(td.AverageTemperature AS FLOAT) AS avg_temp,
            ds.state_code
        FROM
            temperature_data AS td
        JOIN
            state_data AS ds ON td.State = ds.State
    """)
    output_path = output_data + 'dim_temperature/'
    dim_temperature_table.write.mode('overwrite').partitionBy("state_code").parquet(output_path)
    
def extract_and_load_dim_demographics(spark, output_data):
    
    """
    Extract columns to create demographics table.
    Write demographics table to parquet files partitioned by state_code.
    """
    
    dim_demographics_table = spark.sql("""
        SELECT
            State_Code AS state_code,
            CAST(AVG(CAST(Median_Age AS FLOAT)) AS FLOAT) AS age_median,
            SUM(CAST(Male_Population AS INT)) AS m_population,
            SUM(CAST(Female_Population AS INT)) AS f_population,
            SUM(CAST(Total_Population AS INT)) AS population,
            SUM(CAST(Number_of_Veterans AS INT)) AS veterans,
            SUM(CAST(Foreign_born AS INT)) AS foreign_born,
            AVG(CAST(Average_Household_Size AS FLOAT)) AS avg_household
        FROM
            demographics_data
        GROUP BY
            State_Code
    """)
    output_path = output_data + 'dim_demographics/'
    dim_demographics_table.write.mode('overwrite').partitionBy("state_code").parquet(output_path)