from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkConf
from pyspark.sql.types import DateType
from pyspark.sql import DataFrame, functions as sf

def load():

    file_loc = 's3a://dataminded-academy-capstone-resources2/raw/open_aq/'

    jar_pkgs = [
        "org.apache.hadoop:hadoop-aws:3.1.2",
        "net.snowflake:spark-snowflake_2.12:2.9.0-spark_3.1",
        "net.snowflake:snowflake-jdbc:3.13.3"
    ] 

    config = {
        'spark.jars.packages' : ','.join(jar_pkgs),
        'fs.s3a.aws.credentials.provider' : 'com.amazonaws.auth.DefaultAWSCredentialsProviderChain'
    }

    conf = SparkConf().setAll(config.items())
    spark = SparkSession.builder.config(conf = conf).getOrCreate()


    frame = spark.read.json(file_loc)


    #unnest coordinates
    frame = frame.withColumn("latitude", sf.col("coordinates.latitude"))
    frame = frame.withColumn("longitude", sf.col("coordinates.longitude"))

    #unnest date
    frame = frame.withColumn("local_date", sf.col("date.local"))
    frame = frame.withColumn("utc", sf.col("date.utc"))

    frame = frame.select("city", "latitude", "longitude", "country", "local_date", "entity", "isAnalysis", "isMobile", "location", "locationId", "parameter", "sensorType", "unit", "value")
    frame = frame.withColumn("local_date", sf.col("local_date").cast(DateType()))

    return frame