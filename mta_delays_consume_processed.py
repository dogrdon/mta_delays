#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from mta_delays.process.kafka_mta_producer import get_raw_delays_and_send_to_kafka
from mta_delays.ingest.index_es import index_es

defaults = {
    'owner': 'Drew Gordon',
    'depends_on_past': False,
    'catchup': False,
    'start_date': datetime(2018, 5, 15, 10, 42, 59),
    'email':['asgor@uw.edu'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries':1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG('mta_delays_consume_processed', 
          default_args=defaults,
          schedule_interval=timedelta(hours=5))


t1 = PythonOperator(
    task_id="get_raw_delays_and_send_to_kafka",
    python_callable=get_raw_delays_and_send_to_kafka,
    dag=dag
    )

t2 = BashOperator(
    # give time for the consumer to finish before indexing
    task_id='sleep',
    bash_command='sleep 120',
    retries=3,
    dag=dag)

t3 = PythonOperator(
    task_id="index_es",
    python_callable=index_es,
    dag=dag
    )

t3.set_upstream(t2)
t2.set_upstream(t1)
