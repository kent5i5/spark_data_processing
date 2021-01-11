import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """ Extract song, log Data from s3, process them with Spark SQL, and save into another S3 bucket"""
    
    spark = SparkSession \
        .builder \
        .appName("Million Song Spark application") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", os.environ['AWS_ACCESS_KEY_ID']) \
        .config("spark.hadoop.fs.s3a.secret.key", os.environ['AWS_SECRET_ACCESS_KEY']) \
        .getOrCreate()
    return spark


def process_song_and_log_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/A/*/*/*.json")
    
    # read song data file
    df = spark.read.json(song_data)
    
    # create song_data temp View
    df.createOrReplaceTempView("song_data")

    # extract columns to create songs table
    songs_table = spark.sql("SELECT DISTINCT song_id, title, artist_id, year,duration FROM song_data")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data+ "/songs/", mode="overwrite")

    # extract columns to create artists table
    artists_table = artist_table = spark.sql("SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude FROM song_data")
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data+ "/artists/", mode="overwrite")
    
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/2018/11/*.json")

    # read log data file
    df = spark.read.load(log_data)
    
    # create log_data temp View
    df.createOrReplaceTempView("log_data")

    # extract columns for users table    
    user_table = spark.sql("SELECT distinct userId, firstName, lastName, gender, level FROM log_data WHERE page='NextSong'")
    
    #artists_table.collect()
    
    # write users table to parquet files
    user_table.write.parquet(output_data+ "/usrs/", mode="overwrite")

    # Register python datatime.fromtimestamp function to convert ts to timestamp
    spark.udf.register("get_timestamp", lambda x: datetime.fromtimestamp(x / 1000.0).strftime('%Y-%m-%d %H:%M:%S'))

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
          SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT get_timestamp(ts) AS start_time, *
            FROM log_data
            WHERE page='NextSong') events
            LEFT JOIN song_data songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)
    
    
    # create songplays temp table View
    songplays_table.createOrReplaceTempView("song_plays")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data+ "/song_plays/", mode="overwrite")
    
    # extract columns to create time table
    time_table = spark.sql("""
    SELECT start_time, hour(start_time) as hour, 
    dayofmonth(start_time) as day, weekofyear(start_time) as week, 
    month(start_time) as month, year(start_time) as year, 
    dayofweek(start_time) as dayofweek
    FROM song_plays
    """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data+ "/time/", mode="overwrite")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://etl-song-data"
    
    process_song_and_log_data(spark, input_data, output_data)    
    #process_log_data(spark, input_data, output_data)

    spark.stop()


if __name__ == "__main__":
    main()
