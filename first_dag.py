from datetime import  datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def myfirst_func():
    print("its a first program")
    return "ita a first program"


with DAG(dag_id="first_dag",
         schedule_interval="@daily",
         default_args={
             "owner":"ronit",
             "retries":1,
             "retry_delay":timedelta(minutes=5),
             "start_date":datetime(2023,7,5)
         },
         catchup=False) as f:
    myfirst_func =PythonOperator(
        task_id="myfirst_func",
        python_callable=myfirst_func)