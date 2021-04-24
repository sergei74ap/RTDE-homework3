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
    dag_id=USERNAME + '_dwh_etl_mini',
    default_args=default_args,
    default_view='graph',
    description='DWH ETL minimal tasks by ' + USERNAME,
    schedule_interval="@yearly",
    max_active_runs=1,
    params={'schemaName': USERNAME},
)

## ОПИШЕМ ВСЕ ОПЕРАЦИИ ЗАГРУЗКИ ДАННЫХ

ods_reload = PostgresOperator(
    task_id="ods_reload", 
    dag=dag,
    sql="""
delete from {{ params.schemaName }}.ods_t_payment cascade
where extract(year from pay_date) = {{ execution_date.year }};

insert into {{ params.schemaName}}.ods_t_payment
    (select stg.*,
            '{{ execution_date }}'::date as load_dts,
            'PAYMENT_DATALAKE'::text as rec_source
     from {{ params.schemaName }}.stg_t_payment as stg
     where extract(year from stg.pay_date) = {{ execution_date.year }});
"""
)

dds_hub_user = PostgresOperator(
    task_id="dds_hub_user",
    dag=dag,
    sql="""
insert into {{ params.schemaName }}.dds_t_hub_user 
(select * from {{ params.schemaName }}.dds_v_hub_user_etl);
"""
)

dds_sat_user = PostgresOperator(
    task_id="dds_sat_user",
    dag=dag,
    sql="""
insert into {{ params.schemaName }}.dds_t_sat_user 
(select * from {{ params.schemaName }}.dds_v_sat_user_etl);
"""
)

## ОПРЕДЕЛИМ СТРУКТУРУ DAG'А

ods_reload >> dds_hub_user
dds_hub_user >> dds_sat_user
