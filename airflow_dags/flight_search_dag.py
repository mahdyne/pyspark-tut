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
dag = DAG(dag_id='flight_search_dag',
          default_args=default_args,
          catchup=False,
          schedule_interval="0 * * * *")
pyspark_app_home = Variable.get("PYSPARK_APP_HOME")

flight_search_ingestion = SparkSubmitOperator(task_id='flight_search_ingestion',
                                              conn_id='spark_local',
                                              application=f'{pyspark_app_home}/spark/search_event_ingestor.py',
                                              total_executor_cores=4,
                                              packages="io.delta:delta-core_2.12:0.7.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0",
                                              executor_cores=2,
                                              executor_memory='5g',
                                              driver_memory='5g',
                                              name='flight_search_ingestion',
                                              execution_timeout=timedelta(minutes=10),
                                              dag=dag
                                              )

flight_search_waiting_time = SparkSubmitOperator(task_id='flight_search_waiting_time',
                                                 conn_id='spark_local',
                                                 application=f'{pyspark_app_home}/spark/flight_search_waiting_time.py',
                                                 total_executor_cores=4,
                                                 packages="io.delta:delta-core_2.12:0.7.0,org.postgresql:postgresql:42.2.9",
                                                 executor_cores=2,
                                                 executor_memory='10g',
                                                 driver_memory='10g',
                                                 name='flight_search_waiting_time',
                                                 execution_timeout=timedelta(minutes=10),
                                                 dag=dag
                                                 )

flight_nb_search = SparkSubmitOperator(task_id='flight_nb_search',
                                       conn_id='spark_local',
                                       application=f'{pyspark_app_home}/spark/nb_search.py',
                                       total_executor_cores=4,
                                       packages="io.delta:delta-core_2.12:0.7.0,org.postgresql:postgresql:42.2.9",
                                       executor_cores=2,
                                       executor_memory='10g',
                                       driver_memory='10g',
                                       name='flight_nb_search',
                                       execution_timeout=timedelta(minutes=10),
                                       dag=dag
                                       )
flight_search_ingestion >> [flight_search_waiting_time, flight_nb_search]
