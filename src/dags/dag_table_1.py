from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME'] = '/usr'
os.environ['SPARK_HOME'] = '/usr/lib/spark'
os.environ['PYTHONPATH'] = '/usr/local/lib/python3.8'

default_args = {
            'owner': 'airflow',
            'start_date': datetime(2020, 1, 1),
            }

dag_spark = DAG(
            dag_id="sparkoperator_table_1",
            default_args=default_args,
            schedule_interval=None,
            )

# объявляем задачу с помощью SparkSubmitOperator
spark_submit_local = SparkSubmitOperator(
                        task_id='spark_submit_task',
                        dag=dag_spark,
                        application='/lessons/table_1.py',
                        conn_id='yarn_spark',
                        deploy_mode='client',
                        application_args=[
                            '2022-06-21',                 # process_date
                            '90',                         # depth_days
                            '/user/master/data/geo/events/date'  # base_folder
                        ],
                        conf={
                            "spark.driver.memory": "1g",
                            "spark.driver.maxResultSize": "1g",
                            "spark.yarn.am.memory": "2048m",
                            "spark.yarn.am.memoryOverhead": "256m",
                            "spark.yarn.queue": "default"
                        },
                        executor_cores=1,
                        executor_memory='1g'
                        )

spark_submit_local
