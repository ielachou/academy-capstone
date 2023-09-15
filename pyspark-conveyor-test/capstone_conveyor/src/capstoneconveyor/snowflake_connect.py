from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark import SparkConf, SparkContext


def write_to_sf(creds, df):
    print(creds)
    print(type(creds))
    SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
    sfOptions = {
        "sfUrl" : creds["URL"],
        "sfUser" : creds["USER_NAME"],
        "sfPassword" : creds["PASSWORD"],
        "sfSchema" : "Iliass",
        "sfDatabase" : creds["DATABASE"],
        "sfWarehouse" : creds["WAREHOUSE"]
    }
    print(sfOptions)


    df.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable", "weather_report").mode("Overwrite").save()