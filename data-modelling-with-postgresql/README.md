# Data Modeling with Postgres

## Purpose of the database

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

We have created a Postgres database with tables designed to optimize queries on song play analysis and ETL pipeline for this analysis.

## Tables

### songplays
```
+-------------+------------+---------+-------+---------+-----------+------------+----------+------------+
| songplay_id | start_time | user_id | level | song_id | artist_id | session_id | location | user_agent |
+-------------+------------+---------+-------+---------+-----------+------------+----------+------------+
+-----PK------+-----FK-----+---FK----+-------+---FK----+----FK-----+------------+----------+------------+
```

### users
```
+---------+------------+-----------+--------+-------+
| user_id | first_name | last_name | gender | level |
+---------+------------+-----------+--------+-------+
+---PK----+------------+-----------+--------+-------+
```

### songs

```
+---------+-------+-----------+------+----------+
| song_id | title | artist_id | year | duration |
+---------+-------+-----------+------+----------+
+---PK----+-------+-----------+------+----------+
```

### artists

```
+-----------+------+----------+----------+-----------+
| artist_id | name | location | latitude | longitude |
+-----------+------+----------+----------+-----------+
+-----PK----+------+----------+----------+-----------+
```

### time

```
+------------+------+-----+------+-------+------+---------+
| start_time | hour | day | week | month | year | weekday |
+------------+------+-----+------+-------+------+---------+
+-----PK-----+------+-----+------+-------+------+---------+
```

## How to run Python scripts

- Before running scripts, make sure you have a running Python3 kernel.
- On main.ipynb, run the first cell to crete tables for sparkifydb.
- Then run the second cell, to start ETL process to extract the data from the local .json files to our database. 

## Files in the repository

Additional to the files provided,
- main.ipynb: Jupyter Notebook file to create database, schema and tables and to run the ETL pipeline.

## Database schema & ETL pipeline

Schema & ETL pipeline is kept same as provided, only the boilerplate code is filled up.

## Documentation on Function

I haven't declared any extra function other than the ones provided.