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
    USERNAME + '_data_lake_etl2',
    default_args=default_args,
    description='Data Lake ETL_2 by sperfilyev',
    schedule_interval="0 0 1 1 *",
)

ods_billing = DataProcHiveOperator(
    task_id='ods_billing',
    dag=dag,
    query="""
        INSERT OVERWRITE TABLE sperfilyev.ods_billing PARTITION (year='{{ execution_date.year }}') 
        SELECT * FROM sperfilyev.stg_billing WHERE year(created_at)={{ execution_date.year }};    
    """,
    cluster_name='cluster-dataproc',
    job_name=USERNAME + '_ods_billing_{{ execution_date.year }}_{{ params.job_suffix }}',
    params={"job_suffix": randint(0, 100000)},
    region='europe-west3',
)

ods_payment = DataProcHiveOperator(
    task_id='ods_payment',
    dag=dag,
    query="""
        INSERT OVERWRITE TABLE sperfilyev.ods_payment PARTITION (year='{{ execution_date.year }}') 
        SELECT * FROM sperfilyev.stg_payment WHERE year(pay_date)={{ execution_date.year }};    
    """,
    cluster_name='cluster-dataproc',
    job_name=USERNAME + '_ods_payment_{{ execution_date.year }}_{{ params.job_suffix }}',
    params={"job_suffix": randint(0, 100000)},
    region='europe-west3',
)
