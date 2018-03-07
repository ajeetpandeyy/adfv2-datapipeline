from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, DoubleType, DateType
from pyspark.sql.functions import *

spark = SparkSession.builder \
.appName("SparkADF - Simple")\
.enableHiveSupport()\
.getOrCreate()

## Define schema for CSV file
schema = StructType ([
        StructField("date_start", TimestampType(), True),
        StructField("date_stop", TimestampType(), True),
        StructField("server", StringType(), True),
        StructField("ping", DoubleType(), True),
        StructField("download", DoubleType(), True),
        StructField("upload", DoubleType(), True)])

## Read in CSV file ##
df0 = spark.read.csv('wasb://CONTAINERNAME@STORAGEACCOUNT.blob.core.windows.net/input-data/', header=False, schema=schema)

## Cast column 'date_start' from timestamp to date ##
df1 = df0.withColumn('date_start', df0['date_start'].cast('date'))

## Calculate Average Download and Upload by Server ##
df2 = df1.select(col('date_start'), col('server'), col('download'), col('upload')).groupBy('date_start', 'server').agg(avg('download'), avg('upload'))

## Write results to CSV file partitioned by 'server' ##
df2.write.partitionBy('server').mode('append').format('csv').save('wasb://CONTAINERNAME@STORAGEACCOUNT.blob.core.windows.net/output-data/')