import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id, coalesce
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import StructType as Struct, StructField as Field, DoubleType as Double, StringType as String, IntegerType as Int, LongType as Long, DateType as Date, TimestampType as Timestamp 


config = configparser.ConfigParser()
config.read('dl.cfg')
os.environ['AWS_ACCESS_KEY_ID']=config['IAM']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['IAM']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
    Create a Spark Session connected to a Spark cluster
    Returns:
    spark (SparkSession): the Spark Session connected to the Spark cluster.
    '''
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    Process song_data files and populate data into songs and artists tables (parquet files). 
    Parameters:
    spark (SparkSession): the SparkSession connected to the Spark cluster.
    input_data (string): the folder path of the song_data files that is compatible with local file system and cloud storage. All the files under the folder and subfolder will be processed.
    output_data (string): the folder path of the parquet files of songs and artists, and it is compatible with local file system and cloud storage. 
    '''
    
    song_data = input_data + "song_data/*/*/*"       
    df = spark.read.load(song_data, format="json")    
   
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").dropDuplicates(("song_id",))
    songs_table.write.partitionBy("year","artist_id").mode("overwrite").save(output_data+"Songs.parquet")
    
    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude").dropDuplicates(("artist_id",))\
                    .withColumnRenamed("artist_name","name").withColumnRenamed("artist_location","location")\
                    .withColumnRenamed("artist_latitude","latitude") .withColumnRenamed("artist_longitude","longitude")  
    artists_table.write.mode("overwrite").save(output_data+"Artists.parquet")
    

def process_log_data(spark, input_data, output_data):
    '''
    Process log_data files and populate data into users, time and songplay tables (parquet files). 
    Parameters:
    spark (SparkSession): the SparkSession connected to the Spark cluster.
    input_data (string): the folder path of the log_data files that is compatible with local file system and cloud storage. All the files under the folder and subfolder will be processed.
    output_data (string): the folder path of the parquet files of users, time and songplay, and it is compatible with local file system and cloud storage. 
    '''
    
    log_data = input_data + "log_data/*/*"   
    df = spark.read.load(log_data, format="json")   
    df = df.filter(col("page") == 'NextSong')
       
    users_table = df.select("userId", "firstName", "lastName", "gender", "level").dropDuplicates(("userId",))\
                .withColumnRenamed("userId","user_id").withColumnRenamed("firstName",'first_name').withColumnRenamed("lastName",'last_name')   
    users_table.write.mode("overwrite").save(output_data+"Users.parquet")    
   
    get_datetime = udf(lambda t: datetime.fromtimestamp(t/1000), Timestamp())
    df = df.withColumn("datatime", get_datetime("ts"))    
    time_table = df.select("start_time").dropDuplicates(("start_time",))\
                .withColumn("hour", hour("start_time")).withColumn("day", dayofmonth("start_time"))\
                .withColumn("week", weekofyear("start_time")).withColumn("month", month("start_time"))\
                .withColumn("year", year("start_time")).withColumn("weekday", dayofweek("start_time"))   
    time_table.write.partitionBy("year","month").mode("overwrite").save(output_data+"Time.parquet")
    
    song_df = spark.read.load(output_data+"Songs.parquet")       
    
    songplays_table = df.join(song_df, df.song == song_df.title, how="left").withColumn("songplay_id", monotonically_increasing_id())\
                .select("songplay_id","ts","userId","level","song_id","artist_id","sessionId","location","userAgent", year("timestamp").alias("year"),month("timestamp").alias("month"))\
                .withColumnRenamed("ts","start_time").withColumnRenamed("userId","user_id")\
                .withColumnRenamed("sessionId","session_id").withColumnRenamed("userAgent","user_agent") 
    songplays_table.write.partitionBy("year", "month").mode("overwrite").save(output_data+"Songplays.parquet")


def main():
    spark = create_spark_session()
    input_data = config['S3']['SOURCE']
    output_data = config['S3']['DESTINATION']
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
