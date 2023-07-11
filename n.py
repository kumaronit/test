from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

from datetime import datetime, timedelta

def message():
        print("Pyspark airflow job scheduling ...")

default_args = {
                    'owner': 'atif',
                        'start_date': datetime(2022,5,1),
                            'retries': 0,
                                'catchup': False,
                                    'retry_delay': timedelta(minutes=5),
                                    }

dag= DAG('pyspark_job', default_args= default_args, schedule_interval= '*/1 * * * *')

python_operator= PythonOperator(task_id= 'message', python_callable= message, dag= dag)

spark_config= {
                    'conf': {
                                "spark.yarn.maxAppAttemps": "1",
                                        "spark.yarn.executor.memoryOverhead": "512"
                                            },
                        'conn_id': 'spark_local',
                            'application': '/home/ronkumar/SparkTransformation.py', #TODO
                            }

spark_operator= SparkSubmitOperator(task_id= 'spark_submit_task',dag= dag, **spark_config)

python_operator.set_downstream(spark_operator)