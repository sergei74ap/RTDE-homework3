from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataProcHiveOperator

USERNAME = 'sperfilyev'

default_args = {
    'owner': USERNAME,
    'start_date': datetime(2013, 1, 1, 0, 0, 0),
    'email': ['sergei74ap@mail.ru'],
    'email_on_failure': False,
}

dag = DAG(
    USERNAME + '_data_lake_complete',
    default_args=default_args,
    description='Data Lake Complete ETL by sperfilyev',
    schedule_interval="@yearly",
)


def default_ods_fill(tbl_name, fld_partition, flds_to_import='*'): 
    return "INSERT OVERWRITE TABLE sperfilyev.ods_" + tbl_name + \
        " PARTITION (year='{{ execution_date.year }}')" + \
        " SELECT " + flds_to_import + " FROM sperfilyev.stg_" + tbl_name + \
        " WHERE year(" + fld_partition + ")={{ execution_date.year }};"


ods_billing = DataProcHiveOperator(
    task_id='ods_billing',
    dag=dag,
    query=default_ods_fill("billing", "created_at"),
    cluster_name='cluster-dataproc',
    job_name=USERNAME + '_ods_billing_{{ execution_date.year }}_{{ params.job_suffix }}',
    params={"job_suffix": randint(0, 100000)},
    region='europe-west3',
)

ods_issue = DataProcHiveOperator(
    task_id='ods_issue',
    dag=dag,
    query=default_ods_fill("issue", "start_time"),
    cluster_name='cluster-dataproc',
    job_name=USERNAME + '_ods_issue_{{ execution_date.year }}_{{ params.job_suffix }}',
    params={"job_suffix": randint(0, 100000)},
    region='europe-west3',
)

ods_payment = DataProcHiveOperator(
    task_id='ods_payment',
    dag=dag,
    query=default_ods_fill("payment", "pay_date"),
    cluster_name='cluster-dataproc',
    job_name=USERNAME + '_ods_payment_{{ execution_date.year }}_{{ params.job_suffix }}',
    params={"job_suffix": randint(0, 100000)},
    region='europe-west3',
)

ods_traffic = DataProcHiveOperator(
    task_id='ods_traffic',
    dag=dag,
    query=default_ods_fill(
        "traffic", 
        "from_unixtime(floor(`timestamp`/1000))", 
        "user_id, from_unixtime(floor(`timestamp`/1000)), device_id, device_ip_addr, bytes_sent, bytes_received"
    ),
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
        FROM sperfilyev.ods_traffic WHERE year(`timestamp`)={{ execution_date.year }} GROUP BY user_id;    
    """,
    cluster_name='cluster-dataproc',
    job_name=USERNAME + '_dm_traffic_{{ execution_date.year }}_{{ params.job_suffix }}',
    params={"job_suffix": randint(0, 100000)},
    region='europe-west3',
)

ods_traffic >> dm_traffic