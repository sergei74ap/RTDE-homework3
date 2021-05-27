from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators.ssh_execute_operator import SSHExecuteOperator
from airflow.contrib.hooks import SSHHook

USERNAME = 'sperfilyev'

default_args = {
    'owner': USERNAME,
    'start_date': datetime(2013, 1, 1, 0, 0, 0)
}

dag = DAG(
    dag_id=USERNAME + '_dwh_dq',
    default_args=default_args,
    default_view='graph',
    description='DWH DQ tasks by ' + USERNAME,
    schedule_interval="@yearly",
    max_active_runs=1,
    params={'schemaName': USERNAME},
)

sshHook = SSHHook(conn_id='https://pythonanywhere.com')

dq_check = SSHExecuteOperator(
        task_id='dq_check',
        bash_command='ls -al',
        ssh_hook=sshHook,
        dag=dag)
