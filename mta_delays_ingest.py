#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from mta_delays.ingest.ingest_mta_delays import ingest_mta_delays
from mta_delays.ingest.ingest_nyc_weather import ingest_nyc_weather
from mta_delays.ingest.ingest_mta_gtfs import ingest_mta_gtfs

defaults = {
    'owner': 'Drew Gordon',
    'depends_on_past': False,
    'catchup': False,
    'start_date': datetime.now(),
    'email':['asgor@uw.edu'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries':1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('mta_delays_ingest', 
          default_args=defaults,
          schedule_interval=timedelta(seconds=90))


t1 = PythonOperator(
    task_id="ingest_mta_delays",
    python_callable=ingest_mta_delays,
    dag=dag
    )

t2 = PythonOperator(
    task_id="ingest_nyc_weather",
    python_callable=ingest_nyc_weather,
    dag=dag
    )

t3 = PythonOperator(
    task_id="ingest_mta_gtfs",
    python_callable=ingest_mta_gtfs,
    dag=dag
    )

t3.set_upstream(t2)
t2.set_upstream(t1)
