from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, date_format
from pyspark.sql.types import StringType, StructType, StructField, DateType, TimestampType

sparkSession = SparkSession.builder \
    .config("spark.driver.maxResultSize", "2000m") \
    .config("spark.sql.shuffle.partitions", 4) \
    .getOrCreate()
delta_path="hdfs://192.168.1.10:9820/delta/stream/search_response"
checkpoint_url= "hdfs://192.168.1.10:9820/delta/stream/checkpoints/search_response"
def ingest_search_event():
    stream=sparkSession\
        .readStream.format("kafka")\
        .option("kafka.bootstrap.servers","localhost:9099")\
        .option("subscribe", "search-event")\
        .option("startingOffsets", "earliest")\
        .load()
    schema=StructType([
        StructField("searched_at",TimestampType()),
        StructField("responsed_at", TimestampType()),
        StructField("channel",StringType()),
        StructField("origin",StringType()),
        StructField("destination", StringType()),
        StructField("departure_date",DateType())
    ])
    stream\
        .select(from_json(col("value").cast(StringType()),schema).alias("json"))\
        .select(col("json.searched_at").alias("searched_at"),
                col("json.responsed_at").alias("responsed_at"),
                col("json.channel").alias("channel"),
                col("json.origin").alias("origin"),
                col("json.destination").alias("destination"),
                col("json.departure_date").alias("departure_date"))\
        .withColumn("year_month", date_format(col("searched_at"), "yyyy-MM"))\
        .writeStream\
        .format("delta")\
        .trigger(once=True)\
        .partitionBy("year_month")\
        .option("checkpointLocation", checkpoint_url)\
        .start(delta_path)\
        .awaitTermination()
def main():
    ingest_search_event()

if __name__ == '__main__':
    main()