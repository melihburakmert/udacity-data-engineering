# Data Warehouse with Amazon Redshift

## Purpose of the database

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

An ETL pipeline is built that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to.

## Tables

### Staging Tables

#### staging_events

```
+--------+------+-----------+--------+---------------+----------+--------+-------+----------+--------+------+--------------+-----------+------+--------+----+-----------+--------+
| artist | auth | firstName | gender | itemInSession | lastName | length | level | location | method | page | registration | sessionId | song | status | ts | userAgent | userId |
+--------+------+-----------+--------+---------------+----------+--------+-------+----------+--------+------+--------------+-----------+------+--------+----+-----------+--------+
+--------+------+-----------+--------+---------------+----------+--------+-------+----------+--------+------+--------------+-----------+------+--------+----+-----------+--------+
```

#### staging_songs

```
+-----------+-----------+-----------------+------------------+-----------------+-------------+---------+-------+----------+------+
| num_songs | artist_id | artist_latitude | artist_longitude | artist_location | artist_name | song_id | title | duration | year |
+-----------+-----------+-----------------+------------------+-----------------+-------------+---------+-------+----------+------+
+-----------+-----------+-----------------+------------------+-----------------+-------------+---------+-------+----------+------+
```

### Fact Tables

#### fact_songplays
```
+-------------+------------+---------+-------+---------+-----------+------------+----------+------------+
| songplay_id | start_time | user_id | level | song_id | artist_id | session_id | location | user_agent |
+-------------+------------+---------+-------+---------+-----------+------------+----------+------------+
+-------------+------------+---------+-------+---------+-----------+------------+----------+------------+
```

### Dimension Tables

#### dim_users
```
+---------+------------+-----------+--------+-------+
| user_id | first_name | last_name | gender | level |
+---------+------------+-----------+--------+-------+
+---------+------------+-----------+--------+-------+
```

#### dim_songs

```
+---------+-------+-----------+------+----------+
| song_id | title | artist_id | year | duration |
+---------+-------+-----------+------+----------+
+---------+-------+-----------+------+----------+
```

#### dim_artists

```
+-----------+------+----------+----------+-----------+
| artist_id | name | location | latitude | longitude |
+-----------+------+----------+----------+-----------+
+-----------+------+----------+----------+-----------+
```

#### dim_time

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
- Test.ipynb: Jupyter Notebook file to create database, schema and tables and to run the ETL pipeline, also to run queries on it to test the process.