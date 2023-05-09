# Data Lake with EMR + Spark + S3

## Purpose of the database

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

## Tables

#### songplays
```
+-------------+------------+---------+-------+---------+-----------+------------+----------+------------+
| songplay_id | start_time | user_id | level | song_id | artist_id | session_id | location | user_agent |
+-------------+------------+---------+-------+---------+-----------+------------+----------+------------+
+-------------+------------+---------+-------+---------+-----------+------------+----------+------------+
```

#### users
```
+---------+------------+-----------+--------+-------+
| user_id | first_name | last_name | gender | level |
+---------+------------+-----------+--------+-------+
+---------+------------+-----------+--------+-------+
```

#### songs

```
+---------+-------+-----------+------+----------+
| song_id | title | artist_id | year | duration |
+---------+-------+-----------+------+----------+
+---------+-------+-----------+------+----------+
```

#### artists

```
+-----------+------+----------+----------+-----------+
| artist_id | name | location | latitude | longitude |
+-----------+------+----------+----------+-----------+
+-----------+------+----------+----------+-----------+
```

#### time

```
+------------+------+-----+------+-------+------+---------+
| start_time | hour | day | week | month | year | weekday |
+------------+------+-----+------+-------+------+---------+
+------------+------+-----+------+-------+------+---------+
```

## How to test

- The User, Roles, Security Group and the Redshift Cluster is already created on the AWS Console.
- Before running scripts, make sure you have a running Python3 kernel.
- Run all the cells in Test.ipynb

## Files in the repository

Additional to the files provided,
- Test.ipynb: Jupyter Notebook file to test the ETL pipeline on a local Spark instance.
- unzip.py: Python script to unzip the sample data to result directory for test purpose.
- read_data_create_view.py: Python module contains methods to read data from the source folder to dataframe and create table views for Spark SQL
- extract_and_load.py: Python module contains methods to extract data from dataframe and write back to parquet files.