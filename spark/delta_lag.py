from pyspark.sql import SparkSession
from pyspark.sql.functions import col,max
import datetime
import sys
class LagException(Exception):
    def __init__(self, msg):
        self.msg = msg
    def __str__(self):
        return repr(self.msg)
sparkSession = SparkSession.builder \
    .config("spark.driver.maxResultSize", "2000m") \
    .config("spark.sql.shuffle.partitions", 4) \
    .getOrCreate()
def check_lag(delta_path:str,timestamp_column:str,threshold_in_min:int):
    df = sparkSession.read.format("delta").load(delta_path)
    latest_ts=df.select(max(col(timestamp_column))).first()[0]
    now=datetime.datetime.now()
    lag=now-latest_ts
    lag_in_min=divmod(lag.total_seconds(), 60)[0]
    if lag_in_min>threshold_in_min:
        raise LagException(f'The lag of dataset {delta_path} was {lag_in_min} min when checked at {now}')
    else:
        print(f"No lag in dataset {delta_path} with specified threshold {threshold_in_min}")
    sparkSession.stop()
def main():
    delta_path=sys.argv[1]
    timestamp_column=sys.argv[2]
    threshold_in_min=int(sys.argv[3])
    check_lag(delta_path,timestamp_column,threshold_in_min)

if __name__ == '__main__':
    main()