from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataProcHiveOperator

USERNAME = 'sperfilyev'

default_args = {
    'owner': USERNAME,
    'start_date': datetime(2013, 1, 1, 0, 0, 0),
    'email': ['sergei74ap@mail.ru'],
    'email_on_failure': True,   
}

dag = DAG(
    USERNAME + '_data_lake_etl3_dm',
    default_args=default_args,
    description='Data Lake ETL_3 by sperfilyev',
    schedule_interval='@yearly',
)

ods_traffic = DataProcHiveOperator(
    task_id='ods_traffic',
    dag=dag,
    query="""
        INSERT OVERWRITE TABLE sperfilyev.ods_traffic PARTITION (year='{{ execution_date.year }}') 
        SELECT user_id, from_unixtime(floor(`timestamp`/1000)), device_id, device_ip_addr, bytes_sent, bytes_received 
        FROM sperfilyev.stg_traffic WHERE year(from_unixtime(floor(`timestamp`/1000)))={{ execution_date.year }};    
    """,
    cluster_name='cluster-dataproc',
    job_name=USERNAME + '_ods_traffic_{{ execution_date.year }}_{{ params.job_suffix }}',
    params={"job_suffix": randint(0, 100000)},
    region='europe-west3',
)

dm_traffic = DataProcHiveOperator(
    task_id='dm_traffic',
    dag=dag,
    query="""
        INSERT OVERWRITE TABLE sperfilyev.dm_traffic PARTITION (year='{{ execution_date.year }}') 
        SELECT user_id, min(bytes_received), max(bytes_received), avg(bytes_received), current_timestamp, current_user() 
        FROM sperfilyev.ods_traffic WHERE year(`timestamp`)={{ execution_date.year }};    
    """,
    cluster_name='cluster-dataproc',
    job_name=USERNAME + '_dm_traffic_{{ execution_date.year }}_{{ params.job_suffix }}',
    params={"job_suffix": randint(0, 100000)},
    region='europe-west3',
)

ods_traffic >> dm_traffic
