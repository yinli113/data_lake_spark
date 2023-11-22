#!/usr/bin/env python
# coding: utf-8

# SPARK ETL

# In[1]:


# Import necessary libraries
import configparser
from datetime import datetime
import os
from pyspark.sql.types import TimestampType
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import(year, month, dayofmonth,
                                  hour, weekofyear, date_format)
from pyspark.sql import Window

# In[2]:

# configure the saprk setting
config = configparser.ConfigParser()
config.read('dl.cfg')
os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']

# In[3]:


def create_spark_session():
    """
    Create a Spark session.

    Returns:
        spark (pyspark.sql.SparkSession): A Spark session.
    """
    spark = SparkSession.builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

# In[4]:


def process_song_data(spark, input_data, output_data):
    """
    Process song data.

    Args:
        spark (pyspark.sql.SparkSession): A Spark session.
        input_data (str): Input data path.
        output_data (str): Output data path.

    This function reads JSON files containing song data,
    extracts relevant columns, and creates the "songs" and "artists" tables.
    It writes the resulting dataframes to Parquet files.
    """
    song_data = input_data + "song_data/A/A/A/*.json"
    df_song = spark.read.json(song_data)
    df_song.printSchema()
    df_song.take(3)

    # create a temporary table
    df_song.createOrReplaceTempView("t_song")
    songs_table = df_song.select("song_id", "title", "artist_id",
                                 "year", "duration")\
                .filter(df_song.song_id.isNotNull())

    try:
        songs_table.write.mode("overwrite").partitionBy("year", "artist_id") \
            .parquet(os.path.join(output_data, "songs/songs.parquet"))
        print("Songs write completed successfully")
    except Exception as e:
        print(f"Error writing songs table: {str(e)}")

    # extract columns to create artists table
    artists_table = df_song.select("artist_id",
                                   "artist_name",
                                    "artist_location",
                                    "artist_latitude",
                                    "artist_longitude")\
                    .withColumnRenamed("artist_name", "name")\
                    .withColumnRenamed("artist_location", "location") \
                    .withColumnRenamed("artist_latitude", "latitude") \
                    .withColumnRenamed("artist_longitude", "longitude")

    # write artists table to parquet files
    try:
        artists_table.write.mode("overwrite").\
            parquet(os.path.join(output_data, "artists/artists.parquet"))
        print("Artists write completed successfully")
    except Exception as e:
        print(f"Error writing artists table: {str(e)}")

# In[7]:


def process_log_data(spark, input_data, output_data):
    """
    Process log data.

    Args:
        spark (pyspark.sql.SparkSession): A Spark session.
        input_data (str): Input data path.
        output_data (str): Output data path.

    This function reads JSON files containing log data,
    filters them for "NextSong" actions,
    and creates the "users," "time," and "songplays" tables.
    It writes the resulting dataframes to Parquet files.
    """
    log_data = input_data + "log_data/*/*/*.json"
    df_log = spark.read.json(log_data)

    df_log = df_log.filter(df_log.page == 'NextSong')
    users_table = df_log.select("userId", "firstName",
                                "lastName", "gender", "level")
    users_table = users_table.withColumnRenamed("userId", "user_id")
    users_table = users_table.withColumnRenamed("firstName", "first_name")
    users_table = users_table.withColumnRenamed("lastName", "last_name")

    # write users table to parquet files
    try:
        users_table.write.mode("overwrite").\
            parquet(os.path.join(output_data, "users/userss.parquet"))
        print("users write completed successfully")
    except Exception as e:
        print(f"Error writing users table: {str(e)}")

    # use udf to transform a millisecond int to timestamptype
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000.0),
                                                    TimestampType())
    df_log = df_log.withColumn("timestamp", get_timestamp(df_log.ts))

    # create datetime column from original timestamp column and
    # sets the microsecond component of the timestamp to zero
    get_datetime = udf(lambda x : datetime.fromtimestamp(x / 1000).
                    replace(microsecond=0), TimestampType())

    df_datetime = df_log.withColumn("datetime", get_datetime(df_log.ts))

    time_table = (df_datetime
                .withColumn("hour", hour("datetime"))
                .withColumn("day", dayofmonth("datetime"))
                .withColumn("week", weekofyear("datetime"))
                .withColumn("month", month("datetime"))
                .withColumn("year", year("datetime"))
                .withColumn("weekday", date_format("datetime", "u"))
                .select("datetime", "hour", "day", "week", "month",
                        "year", "weekday")
                .distinct())

    try:
        # Write time table to parquet files partitioned by year and month
        time_table.write.mode("overwrite").partitionBy("year", "month").\
                    parquet(os.path.join(output_data, "time/time.parquet"))
        print("Time write completed successfully")
    except Exception as e:
        print(f"Error writing Time table: {str(e)}")
    
    # extract columns from joined song and log datasets
    # to create songplays table
    df_datetime.createOrReplaceTempView("t_log")

    songplays_table = spark.sql("""
    SELECT "datetime",
           "userId",
           "level",
           "song_id",
           "artist_id",
           "sessionId",
           "location",
           "userAgent",
           year(l.datetime) as year,
           month(l.datetime) as month
    FROM t_song s
    LEFT JOIN t_log l ON l.song == s.title and
                        l.artist == s.artist_name and
                        l.length == s.duration
    WHERE l.page='NextSong'
""")

    songplays_table = songplays_table.\
                    withColumnRenamed("ts", "datetime")
    songplays_table = songplays_table.\
                    withColumnRenamed("userId", "user_id")
    songplays_table = songplays_table.\
                    withColumnRenamed("sessionId", "session_id")
    songplays_table = songplays_table.\
                    withColumnRenamed("userAgent", "user_agent")
    
    # write songplays table to parquet files partitioned by year and month
    try:
        songplays_table.write.mode("overwrite").partitionBy("year", "month")\
            .parquet(os.path.join(output_data, "songplays/songplays.parquet"))
        print("songplays write completed successfully")
    except Exception as e:
        print(f"Error writing songplays table: {str(e)}")

# In[8]:


def main():
    """
    Main function for ETL process.

    This function creates a Spark session,
    defines the input and output data paths,
    and calls the process_song_data and process_log_data functions.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "./Final/"
    
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
