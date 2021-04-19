from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

USERNAME = 'sperfilyev'

default_args = {
    'owner': USERNAME,
    'start_date': datetime(2013, 1, 1, 0, 0, 0)
}

dag = DAG(
    USERNAME + '_dwh_etl',
    default_args=default_args,
    description='DWH ETL tasks',
    schedule_interval="@monthly",
)

ods_load_batch = PostgresOperator(
    task_id="ods_load_payment",
    dag=dag,
    sql="insert into " + USERNAME + ".ods_t_payment "\
        "(select * from " + USERNAME + ".stg_t_payment "\
        "where extract(year from pay_date) = {{ execution_date.year }} "\
        "and extract(month from pay_date) = {{ execution_date.month }});"
)
