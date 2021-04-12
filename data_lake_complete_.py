from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataProcHiveOperator

USERNAME = 'sperfilyev'


def generate_ods_fill(tbl_name, fld_partition, flds_to_import='*'): 
    return "INSERT OVERWRITE TABLE " + USERNAME + ".ods_" + tbl_name + \
        " PARTITION (year='{{ execution_date.year }}')" + \
        " SELECT " + flds_to_import + " FROM " + USERNAME + ".stg_" + tbl_name + \
        " WHERE year(" + fld_partition + ")={{ execution_date.year }};"


def generate_dm_fill(tbl_name, fld_aggr, fld_group='user_id'): 
    return "INSERT OVERWRITE TABLE " + USERNAME + ".dm_" + tbl_name + \
        " PARTITION (year='{{ execution_date.year }}')" + \ 
        " SELECT " + fld_group + ", min(" + fld_aggr + "), max(" + fld_aggr + "), avg(" + \
        fld_aggr + "), current_timestamp, current_user()" + \
        " FROM " + USERNAME + ".ods_" + tbl_name + \
        " WHERE year(`timestamp`)={{ execution_date.year }} GROUP BY " + fld_group + ";"


def generate_ods_job(tbl_name):
    return USERNAME + '_ods_' + tbl_name + '_{{ execution_date.year }}_{{ params.job_suffix }}'


def generate_dm_job(tbl_name):
    return USERNAME + '_dm_' + tbl_name + '_{{ execution_date.year }}_{{ params.job_suffix }}'


metadata_ods = {
    'billing': {'field_of_partition': 'created_at', 'fields_to_import': '*'},
    'issue':   {'field_of_partition': 'start_time', 'fields_to_import': '*'},
    'payment': {'field_of_partition': 'pay_date', 'fields_to_import': '*'},
    'traffic': {
        'field_of_partition': 'from_unixtime(floor(`timestamp`/1000))', 
        'fields_to_import': 'user_id, from_unixtime(floor(`timestamp`/1000)), '
                            'device_id, device_ip_addr, bytes_sent, bytes_received'
    },
}

metadata_dm = {
    'traffic': {'field_aggregate': 'bytes_received', 'field_group_by': 'user_id'},
}

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

ods_tasks = {}
for ods_table in metadata_ods.keys():
    ods_tasks[ods_table] = DataProcHiveOperator(
        query=generate_ods_fill(
            ods_table, 
            metadata_ods[ods_table]['field_of_partition'],
            metadata_ods[ods_table]['fields_to_import'],
        ),
        cluster_name='cluster-dataproc',
        job_name=generate_ods_job(ods_table),
        params={"job_suffix": randint(0, 100000)},
        region='europe-west3',
        task_id='ods_' + ods_table,
        dag=dag,
    )

dm_tasks = {}
for dm_table in metadata_dm.keys():
    dm_tasks[dm_table] = DataProcHiveOperator(
        query=generate_dm_fill(
            dm_table,
            metadata_dm[dm_table]['field_aggregate'],
            metadata_dm[dm_table]['field_group_by'],
        ),
        cluster_name='cluster-dataproc',
        job_name=generate_dm_job(dm_table),
        params={"job_suffix": randint(0, 100000)},
        region='europe-west3',
        task_id='dm_' + dm_table,
        dag=dag
    )

ods_tasks['traffic'] >> dm_tasks['traffic']
