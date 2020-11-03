import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, avg, date_format,concat
from pyspark.sql.types import DoubleType, TimestampType, DateType

sparkSession = SparkSession.builder \
    .config("spark.driver.maxResultSize", "2000m") \
    .config("spark.sql.shuffle.partitions", 4) \
    .getOrCreate()

delta_path="hdfs://192.168.1.10:9820/delta/stream/search_response"
pg_data_url = "jdbc:postgresql://192.168.10.10:5432/dw"
pg_data_user = "report"
pg_data_password = "789456123"
pg_driver = 'org.postgresql.Driver'
result_dataset = "flight_search_count"
def flight_search_count():
    search_waiting_time = sparkSession.read.format("delta").load(delta_path) \
        .select(col("searched_at").cast(DateType()).alias("date"),
                concat(col("origin"),'-',col("destination")).alias("route"),
                col("channel"),
                col("departure_date"))\
        .groupBy('date','route','channel')\
        .count()
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
    flight_search_count()


if __name__ == '__main__':
    t1 = datetime.datetime.now()
    print('started at :', t1)
    main()
    t2 = datetime.datetime.now()
    dist = t2 - t1
    print('finished at:', t2, ' | elapsed time (s):', dist.seconds)
