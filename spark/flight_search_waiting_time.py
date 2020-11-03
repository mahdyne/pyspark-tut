import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, avg, date_format
from pyspark.sql.types import DoubleType, TimestampType

sparkSession = SparkSession.builder \
    .config("spark.driver.maxResultSize", "2000m") \
    .config("spark.sql.shuffle.partitions", 4) \
    .getOrCreate()

delta_path="hdfs://192.168.1.10:9820/delta/stream/search_response"
pg_data_url = "jdbc:postgresql://192.168.10.10:5432/dw"
pg_data_user = "report"
pg_data_password = "789456123"
pg_driver = 'org.postgresql.Driver'
result_dataset = "dom_flight_search_waiting_time"
def flight_search_waiting_time():
    search_waiting_time = sparkSession.read.format("delta").load(delta_path) \
        .select(col("channel"),
                date_format("searched_at","yyyy-MM-dd H:00:00").cast(TimestampType()).alias("date_hour"),
                lit(col("responsed_at").cast(DoubleType())-col("searched_at").cast(DoubleType())).alias("diff")) \
        .groupBy("date_hour","channel").agg(avg(col("diff")).alias("waiting_time_in_sec"))
    search_waiting_time.write.format("jdbc") \
        .option("driver", pg_driver) \
        .option("url", pg_data_url) \
        .option("user", pg_data_user) \
        .option("password", pg_data_password) \
        .option("dbtable", result_dataset) \
        .option("batchsize", 50000) \
        .option("isolationLevel", "NONE") \
        .mode("overwrite") \
        .save()


def main():
    flight_search_waiting_time()


if __name__ == '__main__':
    t1 = datetime.datetime.now()
    print('started at :', t1)
    main()
    t2 = datetime.datetime.now()
    dist = t2 - t1
    print('finished at:', t2, ' | elapsed time (s):', dist.seconds)
