import time
from datetime import datetime
from datetime import timedelta
from random import randint, random
from timeit import default_timer as timer

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from prometheus_client import CollectorRegistry, push_to_gateway, Summary, Counter

GATEWAY = 'pushgateway'


def print_hello():
    registry = CollectorRegistry()
    c = Counter('count_exceptions', 'counts number of successes and failures',
                labelnames=['type'], registry=registry)
    s = Summary('time_delta', 'execution time of print_hello function', registry=registry)

    for i in range(randint(1, 10)):
        start = timer()
        time.sleep(random()*10)

        try:

            if randint(0, 1) == 1:
                raise Exception

            c.labels(type='success').inc()

        except:
            c.labels(type='failure').inc()

        end = timer()
        s.observe(timedelta(seconds=end - start).seconds)

    push_to_gateway('%s:9091' % GATEWAY, job='print_hello', registry=registry)

    return 'Hello world!'


dag = DAG('hello_world',
          description='Simple tutorial DAG',
          schedule_interval='*/1 * * * *',
          start_date=datetime(2017, 3, 20),
          catchup=False)

dummy_operator = DummyOperator(task_id='dummy_task', retries=3, dag=dag)

hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)

dummy_operator >> hello_operator
