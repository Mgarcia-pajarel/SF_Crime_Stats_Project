import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf
from dateutil.parser import parse as parse_date


# Creating schema for incoming resources

schema = StructType([
    StructField('crime_id', StringType(), True),
    StructField('original_crime_type_name', StringType(), True),
    StructField('report_date', StringType(), True),
    StructField('call_date', StringType(), True),
    StructField('offense_date', StringType(), True),
    StructField('call_time', StringType(), True),
    StructField('call_date_time', StringType(), True),
    StructField('disposition', StringType(), True),
    StructField('address', StringType(), True),
    StructField('city', StringType(), True),
    StructField('state', StringType(), True),
    StructField('agency_id', StringType(), True),
    StructField('address_type', StringType(), True),
    StructField('common_location', StringType(), True)
])

# Creating a spark udf to convert time to YYYYmmDDhh format

@psf.udf(StringType())
def udf_convert_time(timestamp):
    date = parse_date(timestamp)
    return str(date.strftime('%y%m%d%H'))

def run_spark_job(spark):
    
    print('running spark job')

# Creating Spark Configuration with max offset of 200 per trigger and correct bootstrap server/port
    
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "com.udacity.sf.crime.calls") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", "200") \
        .option("maxRatePerPartition", "2") \
        .option("stopGracefullyOnShutdown", "true") \
        .load()

# Showing schema for the incoming resources for checks
    
    print('======= printSchema ========')
    
    df.printSchema()

# Extracting the correct column from the kafka input resources
# Take only value and convert it to String
    
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("CALLS"))\
        .select("CALLS.*")

# Selecting original_crime_type_name and disposition
    
    print('======= printSchema ========')
    
    distinct_table = service_table.select(psf.col('crime_id'),
                psf.col('original_crime_type_name'),
                psf.to_timestamp(psf.col('call_date_time')).alias('call_datetime'),
                psf.col('address'),
                psf.col('disposition'))

    # count the number of original crime type
    
    agg_df = distinct_table\
        .withWatermark("call_datetime", "60 minutes")\
        .groupBy(psf.window(distinct_table.call_datetime, "10 minutes", "5 minutes"),distinct_table.original_crime_type_name)\
        .count()

    print('=== agg_df')

    # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # TODO write output stream
    query = agg_df \
        .writeStream \
        .format('console') \
        .outputMode('Complete') \
        .trigger(processingTime="10 seconds") \
        .start()


    # TODO attach a ProgressReporter
    print('=== awaitTermination ===')
    query.awaitTermination()

    # TODO get the right radio code json path
    print('=== radio_code....===')
    radio_code_json_filepath = "radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath)

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # TODO rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition").collect()

    # TODO join on disposition column
    join_query = agg_df.join(radio_code_df, col("agg_df.disposition") == col("radio_code_df.disposition"), "left_outer")

    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .config("spark.ui.port", 3000) \
        .master("local[*]") \
        .appName("Miguel Garcia-Gosalvez/ SF Crime Statistics") \
        .getOrCreate()

    spark.sparkContext.getConf().getAll()   

    logger.info("Spark Started")

    run_spark_job(spark)
    
    logger.info("Spark Stopped")
    spark.stop()
