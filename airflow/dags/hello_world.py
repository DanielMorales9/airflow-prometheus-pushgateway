import time
from datetime import datetime
from random import randint, random

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway


def print_hello():
    registry = CollectorRegistry()
    g = Gauge('gauge', 'counts success or failures', registry=registry)

    for i in range(randint(10)):
        time.sleep(random())
        try:
            if randint(0, 1) == 1:
                raise Exception
            g.labels('type', 'success').inc()
        except Exception:
            g.labels('type', 'failure').inc()

    push_to_gateway('pushgateway:9091', job='print_hello', registry=registry)
    return 'Hello world!'


dag = DAG('hello_world',
          description='Simple tutorial DAG',
          schedule_interval='*/1 * * * *',
          start_date=datetime(2017, 3, 20),
          catchup=False)

dummy_operator = DummyOperator(task_id='dummy_task', retries=3, dag=dag)

hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)

dummy_operator >> hello_operator
