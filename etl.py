import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DecimalType as Dcl

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
    
    # write users table to parquet files
    artists_table

    # create timestamp column from original timestamp column
    get_timestamp = udf()
    df = 
    
    # create datetime column from original timestamp column
    get_datetime = udf()
    df = 
    
    # extract columns to create time table
    time_table = 
    
    # write time table to parquet files partitioned by year and month
    time_table

    # read in song data to use for songplays table
    song_df = 

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = 

    # write songplays table to parquet files partitioned by year and month
    songplays_table


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
