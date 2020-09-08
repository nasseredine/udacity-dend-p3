import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import udf, col, row_number
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DecimalType as Dcl, TimestampType as Ts

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates a SparkSession object with AWS access and secret key.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("spark.hadoop.fs.s3a.awsAccessKeyId", os.environ['AWS_ACCESS_KEY_ID']) \
        .config("spark.hadoop.fs.s3a.awsSecretAccessKey", os.environ['AWS_SECRET_ACCESS_KEY']) \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Processes song data and creates songs and artists tables.
    Args:
        spark (SparkSession): The SparkSession object.
        input_data (str): Input data location in Amazon S3.
        output_data (str): Output data location in Amazon S3.
    """
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")
    
    song_schema = R([
        Fld("artist_id", Str()),
        Fld("artist_latitude", Dcl(8,5)),
        Fld("artist_location", Str()),
        Fld("artist_longitude", Dcl(8,5)),
        Fld("artist_name", Str()),
        Fld("duration", Dcl(9,5)),
        Fld("num_songs", Int()),
        Fld("song_id", Str()),
        Fld("title", Str()),
        Fld("year", Int()),
    ])
    
    df = spark.read.json(song_data, schema=song_schema)

    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").where(col("song_id").isNotNull())
    
    songs_table.write.partitionBy("year", "artist_id").parquet(os.path.join(output_data, "songs.parquet"))

    df.createOrReplaceTempView("song_data")
    artists_table = spark.sql("""
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM song_data AS sd1
    WHERE sd1.artist_id IS NOT NULL AND sd1.year = (SELECT MAX(sd2.year)
                                                    FROM song_data AS sd2
                                                    WHERE sd2.artist_id = sd1.artist_id)
    """)
    
    artists_table.write.parquet(os.path.join(output_data, "artists.parquet"))


def process_log_data(spark, input_data, output_data):
    """Processes song data and creates users, time and songplays tables.
    Args:
        spark (SparkSession): The SparkSession object.
        input_data (str): Input data location in Amazon S3.
        output_data (str): Output data location in Amazon S3.
    """
    log_data = "s3a://udacity-dend/log_data"

    df = spark.read.json(log_data)
    df = df.filter(col("page") == "NextSong")
    
    df.createOrReplaceTempView("log_data")
    users_table = spark.sql("""
    SELECT DISTINCT userId AS user_id, firstName AS first_name, lastName as last_name, gender, level
    FROM log_data AS ld1
    WHERE ld1.userId IS NOT NULL AND ld1.ts = (SELECT MAX(ld2.ts)
                                               FROM log_data AS ld2
                                               WHERE ld2.userId = ld1.userId )
    """)
    
    users_table.write.parquet(os.path.join(output_data, "users.parquet"))

    get_timestamp = udf(lambda ms: datetime.fromtimestamp(ms/1000.0), Ts())
    spark.udf.register("get_timestamp", get_timestamp)
    df = df.withColumn("start_time", get_timestamp(col("ts")))
    
    window = Window.orderBy("userId", "sessionId", "itemInSession")
    df = df.withColumn('songplay_id', row_number().over(window))
    
    time_table = spark.sql("""
    SELECT distinct start_time,
            hour(start_time) AS hour,
            dayofmonth(start_time) AS day,
            weekofyear(start_time) AS week,
            month(start_time) AS month,
            year(start_time) AS year,
            dayofweek(start_time) AS weekday
    FROM log_data
    WHERE start_time IS NOT NULL
    """)
    
    time_table.write.partitionBy("year", "month").parquet(os.path.join(output_data, "time.parquet"))

    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")
    song_schema = R([
        Fld("artist_id", Str()),
        Fld("artist_latitude", Dcl(8,5)),
        Fld("artist_location", Str()),
        Fld("artist_longitude", Dcl(8,5)),
        Fld("artist_name", Str()),
        Fld("duration", Dcl(9,5)),
        Fld("num_songs", Int()),
        Fld("song_id", Str()),
        Fld("title", Str()),
        Fld("year", Int()),
    ])

    song_df = spark.read.json(song_data, schema=song_schema)

    song_df.createOrReplaceTempView("song_data")
    songplays_table = spark.sql("""
    SELECT  ld.songplay_id,
            ld.start_time,
            ld.userId as user_id,
            ld.level,
            sd.artist_id,
            ld.sessionId as session_id,
            sd.artist_location AS location,
            ld.userAgent AS userAgent
    FROM log_data AS ld
    JOIN song_data AS sd
    ON ld.song = sd.title AND ld.artist = sd.artist_name AND ld.length = sd.duration
    """)

    songplays_table.write.parquet(os.path.join(output_data, "songplays.parquet"))


def main():
    """
    - Creates a SparkSession object.
    - Processes song data.
    - Processes log data.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://nasseredine-dend/data_lake/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
