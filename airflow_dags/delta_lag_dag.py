from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import Variable

local_tz = pendulum.timezone("Asia/Tehran")

default_args = {
    'owner': 'mahdyne',
    'depends_on_past': False,
    'start_date': datetime(2020, 10, 10, tzinfo=local_tz),
    'email': ['nematpour.ma@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}
dag = DAG(dag_id='delta_lag_dag',
          default_args=default_args,
          catchup=False,
          schedule_interval="30 * * * *")
pyspark_app_home=Variable.get("PYSPARK_APP_HOME")
click_stream_delta_lag_alert= SparkSubmitOperator(task_id='clickstream_delta_lag_alert',
                                     conn_id='spark_local',
                                     application=f'{pyspark_app_home}/spark/delta_lag.py',
                                     total_executor_cores=4,
                                     packages="io.delta:delta-core_2.12:0.7.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0",
                                     executor_cores=2,
                                     executor_memory='5g',
                                     driver_memory='5g',
                                     name='clickstream_delta_lag_alert',
                                     dag=dag,
                                     application_args=["/home/mahdyne/wd/data/spark_streaming/click_stream","searched_at","12000"]
                                     )

